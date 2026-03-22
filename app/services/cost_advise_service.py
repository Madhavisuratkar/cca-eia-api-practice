import csv
import os
import shutil
import subprocess
from app.middleware.request_context import get_request
from app.utils.common_utils import dollar_spend_eval_from_json, paginate_transformed_data, protect_existing_excel, paginate_collection, reformat_recommendation_data, get_cca_pipeline, get_eia_pipeline, extract_org_and_user_from_email, convert_to_utc, energy_chart_eval_from_flat
from app.utils.constants import AppName, LevelType, ANNUAL_COST, ANNUAL_SAVINGS_I, ANNUAL_SAVINGS_II, ANNUAL_SAVINGS_III, PERF_ENHANCEMENT_I, PERF_ENHANCEMENT_II, PERF_ENHANCEMENT_III, MONTHLY_UTILIZATION, FILE_EXTENSION, CollectionNames, RecommendationStatus
import pandas as pd
from app.connections.env_config import results_path, results_path_url, results_path_url_eia
import os
from datetime import datetime,timezone
from typing import Dict, Any, List, Optional
from app.connections.custom_exceptions import CustomAPIException
from app.utils.cca_ppt_generation import generate_ppt
from app.connections.mongodb import get_collection
from bson import ObjectId
from app.connections.pylogger import log_message
from app.services.portfolios_service import save_portfolio_data, patch_portfolio_data
from app.schema.portfolio_model_without_cloud import SavePortfolioRequest
from app.services.validation_service import cca_validate_input_data, validate_input_data
from app.connections.cloud_s3_connect import generate_download_presigned_url, upload_file_to_s3
from pydantic import TypeAdapter
import asyncio
from app.utils.csv_to_excel_generation import csv_to_excel_generation
from app.models.policy_engine import PolicyEngine
from sqlalchemy import func, or_
from sqlalchemy.orm import Session
from app.utils.cca_excel_generation import generate_excel_from_json
from app.utils.eia_excel_generation import generate_excel_report
from app.utils.eia_ppt_generation import generate_ppt_from_excel

base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # project root

grand_total = {
    'Number of Instances': 0,
    'Current Monthly Cost': 0,
    ANNUAL_COST: 0,
    'Monthly Cost I': 0,
    'Annual Cost I (perf scaled)': 0,
    ANNUAL_SAVINGS_I: 0,
    'Monthly Cost II': 0,
    'Annual Cost II (perf scaled)': 0,
    ANNUAL_SAVINGS_II: 0,
    'Monthly Cost III': 0,
    'Annual Cost III (perf scaled)': 0,
    ANNUAL_SAVINGS_III: 0
}


def run_command(command):
    result = subprocess.run(command, universal_newlines=True, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return result.stdout, result.stderr


def data_collection(input_file, output_file, udf_file, policy_engine_file_path, app_name):
    msg = ""
    try:
        if udf_file:
            command = f"bash run.sh {input_file} {output_file} {app_name.upper()} {udf_file} no"
        elif policy_engine_file_path:
            command = f"bash run.sh {input_file} {output_file} {app_name.upper()} {policy_engine_file_path} no"
        else:
            command = f"bash run.sh {input_file} {output_file} {app_name.upper()} - no"

        out = "STDOUT: "
        err = "STDERR: "
        std_out, std_err = run_command(command)
        msg = out + std_out + err + std_err
        if not is_command_successful(std_out, std_err):
            return msg, None, False

        if os.path.exists(output_file):
            # output_file_name = output_file.replace('.csv', '.xlsx')
            # xl_file = csv_to_excel_generation([output_file], "Recommended-Instance", output_file_name, app_name, results_path)
            if app_name.upper() == 'CCA':
                return cca_process_output_file(output_file, out + std_out, err + std_err)
            else:
                return eia_process_output_file(output_file, out + std_out, err + std_err)
        else:
            return msg, None, False

    except subprocess.TimeoutExpired as err:
        log_message(LevelType.ERROR, str(err), ErrorCode=-1)
        return msg, None, False
    except Exception as err:
        log_message(LevelType.ERROR, str(err), ErrorCode=-1)
        return msg, None, False


def is_command_successful(std_out, std_err):
    if "Error" in std_out or "HDF5-DIAG" in std_out or "terminate called" in std_out or "Segmentation fault" in std_out or "terminated" in std_out:
        return False
    if std_err:
        return False
    return True

def cca_process_data_perf(perf_enhancement_i, perf_enhancement_ii, perf_enhancement_iii, row, key, sums):
    try:
        value = float(row.get(key, 0)) if row.get(key, "").strip() else 0
        sums[key] += value
        
        if row.get(PERF_ENHANCEMENT_I, "").strip() not in ["", "-", "inf"]:
            perf_enhancement_i.append(float(row[PERF_ENHANCEMENT_I]))

        if row.get(PERF_ENHANCEMENT_II, "").strip() not in ["", "-", "inf"]:
            perf_enhancement_ii.append(float(row[PERF_ENHANCEMENT_II]))

        if row.get(PERF_ENHANCEMENT_III, "").strip() not in ["", "-", "inf"]:
            perf_enhancement_iii.append(float(row[PERF_ENHANCEMENT_III]))
        return perf_enhancement_i, perf_enhancement_ii, perf_enhancement_iii, True
    except ValueError:
        return perf_enhancement_i, perf_enhancement_ii, perf_enhancement_iii, False

def cca_process_output_data(dict_data, sums):
    perf_enhancement_i, perf_enhancement_ii, perf_enhancement_iii = [], [], []
    for row in dict_data:
        for key in sums.keys():
            perf_enhancement_i, perf_enhancement_ii, perf_enhancement_iii, flag = cca_process_data_perf(perf_enhancement_i, perf_enhancement_ii, perf_enhancement_iii, row, key, sums)
            if not flag:
                continue
    return perf_enhancement_i, perf_enhancement_ii, perf_enhancement_iii


def cca_process_output_file(output_file, std_out, std_err):

    with open(output_file, 'r') as f:
        reader = csv.reader(f)
        data = list(reader)
        if not data:
            return std_out.strip() + "\n" + std_err.strip(), None, False

        header = [col.strip() for col in data[0]]
        dict_data = [
            {header[i].strip(): row[i].strip() for i in range(min(len(header), len(row)))}
            for row in data[1:] if row
        ]
        sums = {
            'Number of Instances': 0,
            'Current Monthly Cost': 0,
            ANNUAL_COST: 0,
            'Monthly Cost I': 0,
            'Annual Cost I (perf scaled)': 0,
            ANNUAL_SAVINGS_I: 0,
            PERF_ENHANCEMENT_I: 0,
            'Monthly Cost II': 0,
            'Annual Cost II (perf scaled)': 0,
            ANNUAL_SAVINGS_II: 0,
            PERF_ENHANCEMENT_II: 0,
            'Monthly Cost III': 0,
            'Annual Cost III (perf scaled)': 0,
            ANNUAL_SAVINGS_III: 0,
            PERF_ENHANCEMENT_III: 0
        }

        perf_enhancement_i, perf_enhancement_ii, perf_enhancement_iii = cca_process_output_data(dict_data, sums)

        avg_perf_enhancement_i = round(sum(perf_enhancement_i) / len(perf_enhancement_i), 2) if perf_enhancement_i else 0
        avg_perf_enhancement_ii = round(sum(perf_enhancement_ii) / len(perf_enhancement_ii), 2) if perf_enhancement_ii else 0
        avg_perf_enhancement_iii = round(sum(perf_enhancement_iii) / len(perf_enhancement_iii), 2) if perf_enhancement_iii else 0

        sums[PERF_ENHANCEMENT_I] = avg_perf_enhancement_i
        sums[PERF_ENHANCEMENT_II] = avg_perf_enhancement_ii
        sums[PERF_ENHANCEMENT_III] = avg_perf_enhancement_iii

        sums = {key: round(value, 2) for key, value in sums.items()}
        unique_zones = {item.get("Zone") for item in dict_data if item.get("Zone")}
        sums["uniqueZones"] = len(unique_zones)

        return std_out.strip() + "\n" + std_err.strip(), {'data': dict_data, 'grandTotal': sums}, True

    
def eia_process_output_file(output_file, std_out, std_err):

    with open(output_file, 'r') as f:
        reader = csv.reader(f)
        data = list(reader)
        if not data:
            return std_out.strip() + "\n" + std_err.strip(), None, False

        header = [col.strip() for col in data[0]]
        dict_data = [
            {header[i].strip(): row[i].strip() for i in range(min(len(header), len(row)))}
            for row in data[1:] if row
        ]

        # Define numeric fields for grand total
        sums = {
            'Current Monthly Price': 0.0,
            'Current Instance Energy Consumption (kwh)': 0.0,
            'Current Instance Emission': 0.0,
            'Monthly Price I': 0.0,
            'Instance Energy Consumption I (kwh)': 0.0,
            'Instance Emission I': 0.0,
            'Monthly Savings I': 0.0,
            'Monthly Price II': 0.0,
            'Instance Energy Consumption II (kwh)': 0.0,
            'Instance Emission II': 0.0,
            'Monthly Savings II': 0.0,
        }

        perf_enhancement_i = []
        perf_enhancement_ii = []
        untapped_capacity_i = []
        untapped_capacity_ii = []

        # Aggregate totals
        for row in dict_data:
            for key in sums:
                try:
                    value = row.get(key, "").strip()
                    if value not in ["", "-", None]:
                        sums[key] += float(value)
                except ValueError:
                    pass
            
            # Collect performance enhancement values
            try:
                val_i = row.get('Perf Enhancement I', "").strip()
                if val_i not in ["", "-", None]:
                    perf_enhancement_i.append(float(val_i))
            except ValueError:
                pass

            try:
                val_ii = row.get('Perf Enhancement II', "").strip()
                if val_ii not in ["", "-", None]:
                    perf_enhancement_ii.append(float(val_ii))
            except ValueError:
                pass

            try:
                val_uc_i = row.get('Untapped Capacity I', "").strip()
                if val_uc_i not in ["", "-", None]:
                    untapped_capacity_i.append(float(val_uc_i))
            except ValueError:
                pass

            try:
                val_uc_ii = row.get('Untapped Capacity II', "").strip()
                if val_uc_ii not in ["", "-", None]:
                    untapped_capacity_ii.append(float(val_uc_ii))
            except ValueError:
                pass

        # Calculate averages
        sums['Perf Enhancement I'] = round(sum(perf_enhancement_i) / len(perf_enhancement_i), 2) if perf_enhancement_i else 0.0
        sums['Perf Enhancement II'] = round(sum(perf_enhancement_ii) / len(perf_enhancement_ii), 2) if perf_enhancement_ii else 0.0
        sums['Untapped Capacity I'] = round(sum(untapped_capacity_i) / len(untapped_capacity_i), 2) if untapped_capacity_i else 0.0
        sums['Untapped Capacity II'] = round(sum(untapped_capacity_ii) / len(untapped_capacity_ii), 2) if untapped_capacity_ii else 0.0

        # Round totals
        sums = {key: round(value, 2) for key, value in sums.items()}

        unique_zones = {item.get("Zone") for item in dict_data if item.get("Zone")}
        sums["uniqueZones"] = len(unique_zones)

        return std_out.strip() + "\n" + std_err.strip(), {
            'data': dict_data,
            'grandTotal': sums
        }, True


def create_instance_udf_files_from_json(json_data, headroom_value, udf_data, instance_file_path, udf_file_path):
    data = json_data
    udf = udf_data
    udf_file_created = False

    if data:
        skip_fields = {'pavg', 'uavg', 'p95', 'u95', 'instance name'}
        updated_data = [
            {
                k.replace('pricingModel', 'pricing model') if k == 'pricingModel' else k: v
                for k, v in item.items()
                if k not in skip_fields
            } for item in data
        ]

        # Inject headroom% to each row
        for row in updated_data:
            row["headroom%"] = headroom_value
        data_df = pd.DataFrame(updated_data)
        data_df.to_csv(instance_file_path, index=False)

    if udf:
        udf_df = pd.DataFrame(udf)
        udf_df.to_csv(udf_file_path, index=False)
        udf_file_created = True

    return instance_file_path, udf_file_path if udf_file_created else None


def saving_calculation(savings, cost):
    if cost != "-" and savings != '-':
        # if float(savings) > 0:
        #     save_value = float(savings)
        # else:
        #     save_value = 0
        return round((float(savings) / float(cost)) * 100,2)
    else:
        return "-"

def cca_transformed_data(entry, transformed_data, result):
    entry = {k.strip(): v.strip() if isinstance(v, str) else v for k, v in entry.items()}
    try:
        transformed_entry = {
            "id": entry["UUID"],
            "data": {
                "currentPlatform": {
                    "zone": entry["Zone"],
                    "instanceType": entry["Current Instance"],
                    "numberOfInstances": entry["Number of Instances"],
                    "vCPU": entry["vCPU"],
                    "monthlyCost": entry["Current Monthly Cost"],
                    "annualCost": entry[ANNUAL_COST],
                    "cspProvider": entry["CSP"],
                    "pricingModel": entry["Pricing Model"],
                    "status": entry["STATUS"]
                },
                "recommendations": [
                    {
                        "zone": entry["Zone"],
                        "instanceType": entry["Recommendation I Instance"],
                        "vCPU": entry["vCPU I"],
                        "monthlyCost": entry["Monthly Cost I"],
                        "totalCost": entry["Annual Cost I (perf scaled)"],
                        "annualSavings": 0.0 if entry[ANNUAL_SAVINGS_I] == "-0.000000" else entry[ANNUAL_SAVINGS_I],
                        "savingsInPercentage": saving_calculation(entry[ANNUAL_SAVINGS_I], entry[ANNUAL_COST]),
                        "perf": entry["Perf Enhancement I"]
                    },
                    {
                        "zone": entry["Zone"],
                        "instanceType": entry["Recommendation II Instance"],
                        "vCPU": entry["vCPU II"],
                        "monthlyCost": entry["Monthly Cost II"],
                        "totalCost": entry["Annual Cost II (perf scaled)"],
                        "annualSavings": 0.0 if entry[ANNUAL_SAVINGS_II] == "-0.000000" else entry[ANNUAL_SAVINGS_II],
                        "savingsInPercentage": saving_calculation(entry[ANNUAL_SAVINGS_II], entry[ANNUAL_COST]),
                        "perf": entry["Perf Enhancement II"]
                    },
                    {
                        "zone": entry["Zone"],
                        "instanceType": entry["Recommendation III Instance"],
                        "vCPU": entry["vCPU III"],
                        "monthlyCost": entry["Monthly Cost III"],
                        "totalCost": entry["Annual Cost III (perf scaled)"],
                        "annualSavings": 0.0 if entry[ANNUAL_SAVINGS_III] == "-0.000000" else entry[ANNUAL_SAVINGS_III],
                        "savingsInPercentage": saving_calculation(entry[ANNUAL_SAVINGS_III], entry[ANNUAL_COST]),
                        "perf": entry["Perf Enhancement III"]
                    }
                ]
            }
        }

        h_saving_percentage = round((float(result['grandTotal'][ANNUAL_SAVINGS_I]) / (
            max(1, float(result['grandTotal'][ANNUAL_COST]))) * 100), 2)
        m_saving_percentage = round((float(result['grandTotal'][ANNUAL_SAVINGS_II]) / (
            max(1, float(result['grandTotal'][ANNUAL_COST]))) * 100), 2)
        md_saving_percentage = round((float(result['grandTotal'][ANNUAL_SAVINGS_III]) / (
            max(1, float(result['grandTotal'][ANNUAL_COST]))) * 100), 2)
        result['grandTotal']['hSavingsInPercentage'] = h_saving_percentage
        result['grandTotal']['mSavingsInPercentage'] = m_saving_percentage
        result['grandTotal']['mdSavingsInPercentage'] = md_saving_percentage
        transformed_data.append(transformed_entry)
        return transformed_data
    except Exception as e:
        log_message(LevelType.ERROR, str(e), ErrorCode=-1)
        return None

def get_transformed_rec_data(app_flag, result):
    transformed_data = []
    if app_flag.lower() == 'cca':
        for entry in result['data']:
            transformed_data = cca_transformed_data(entry, transformed_data, result)
    else:
        for entry in result['data']:
            entry = {k.strip(): v.strip() if isinstance(v, str) else v for k, v in entry.items()}
            try:
                transformed_entry = {
                    "id": entry["UUID"],
                    "csp": entry["CSP"],
                    "data": {
                        "currentPlatform": {
                            "type": entry["Current Instance"],
                            "cost": entry["Current Monthly Price"],
                            "power": entry["Current Instance Energy Consumption (kwh)"],
                            "carbon": entry["Current Instance Emission"],
                            "status": entry["STATUS"],
                            "vCPU": entry["vCPU"],
                            "pricingModel": entry["Pricing Model"],
                            "region": entry["Zone"]
                        },
                        "recommendations": [
                            {
                                "cost": entry["Monthly Price I"],
                                "type": entry["Recommendation I Instance"],
                                "power": entry["Instance Energy Consumption I (kwh)"],
                                "carbon": entry["Instance Emission I"],
                                "perf": entry["Perf Enhancement I"],
                                "monthlySavings": entry["Monthly Savings I"],
                                "vCPU": entry["vCPU I"],
                                "untappedCapacity": entry["Untapped Capacity I"]
                            },
                            {
                                "cost": entry["Monthly Price II"],
                                "type": entry["Recommendation II Instance"],
                                "power": entry["Instance Energy Consumption II (kwh)"],
                                "carbon": entry["Instance Emission II"],
                                "perf": entry["Perf Enhancement II"],
                                "monthlySavings": entry["Monthly Savings II"],
                                "vCPU": entry["vCPU II"],
                                "untappedCapacity": entry["Untapped Capacity II"]
                            }
                        ]
                    }
                }
                transformed_data.append(transformed_entry)
            except Exception as e:
                log_message(LevelType.ERROR, str(e), ErrorCode=-1)
                return None
    return transformed_data


def costadvise_utils(total_data, input_folder_path, output_folder_path, csv_file_name, policy_data, policy_engine_file_path):
    policy_file_created = False
    summarized_list = [
        {
            "instance type": entry["instance type"],
            "region": entry["region"],
            "quantity": float(entry["quantity"]),
            "monthly utilization": float(entry[MONTHLY_UTILIZATION]),
            "cloud_csp": entry["cloud_csp"],
            "pricing model": entry["pricingModel"],
            "uuid": entry["instance_name"] if "instance_name" in entry and entry["instance_name"].strip() != "" else entry.get("uuid", '')
        } for entry in total_data
    ]
    
    df = pd.DataFrame(summarized_list)
    input_csv_file_path = os.path.join(input_folder_path, csv_file_name)
    output_csv_file_path = os.path.join(output_folder_path, csv_file_name)

    df.to_csv(input_csv_file_path, index=False)
    
    if policy_data:
        policy_df = pd.DataFrame(policy_data)
        # Rename columns to remove underscore
        policy_df = policy_df.rename(columns={
            "instance_type": "instance type",
            "scalar_value": "scalar value"
        })
        policy_df.to_csv(policy_engine_file_path, index=False)
        policy_file_created = True
        
    return input_csv_file_path, output_csv_file_path, policy_engine_file_path if policy_file_created else None


def get_input_headroom(result):
    for row in result['data']:
        val = str(row.get("Input Headroom", "")).strip()
        if val and val != "-":
            try:
                return round(float(val), 2)
            except ValueError:
                continue
    return "-"

async def store_cca_recommendations(transformed_data, portfolio_id, recommended_instance_collection, app_name="CCA"):
    # Ensure data is in list form
    if isinstance(transformed_data, dict):
        transformed_data = [transformed_data]

    # Remove old recommendations for this portfolio
    delete_result = await recommended_instance_collection.delete_many({"portfolio_id": portfolio_id})
    log_message(
        LevelType.INFO,
        f"Deleted {delete_result.deleted_count} old recommendations for portfolio_id={portfolio_id} where app : {app_name}",
        portfolio_id=portfolio_id
    )
    records_to_insert = []
    for idx, rec in enumerate(transformed_data, start=1):
        current = rec.get("data", {}).get("currentPlatform", {})
        recs = rec.get("data", {}).get("recommendations", [])

        rec1 = recs[0] if len(recs) > 0 else {}
        rec2 = recs[1] if len(recs) > 1 else {}
        rec3 = recs[2] if len(recs) > 2 else {}

        records_to_insert.append({
            "portfolio_id": portfolio_id,
            "UUID": rec.get("id", ""),
            "CSP": current.get("cspProvider", "").upper(),
            "Pricing Model": current.get("pricingModel", ""),
            "Zone": current.get("zone", ""),
            "Current Instance": current.get("instanceType", ""),
            "vCPU": current.get("vCPU", ""),
            "Current Monthly Price": current.get("monthlyCost", ""),
            "Annual Cost": current.get("annualCost", ""),
            "Number of Instances": current.get("numberOfInstances", ""),
            "STATUS": current.get("status", ""),

            # Recommendation I
            "Recommendation I Instance": rec1.get("instanceType", ""),
            "vCPU I": rec1.get("vCPU", ""),
            "Monthly Price I": rec1.get("monthlyCost", ""),
            "Annual Cost I": rec1.get("totalCost", ""),
            "Annual Savings I": rec1.get("annualSavings", ""),
            "Savings % I": rec1.get("savingsInPercentage", ""),
            "Perf Enhancement I": rec1.get("perf", ""),
            "Zone I": rec1.get("zone", ""),

            # Recommendation II
            "Recommendation II Instance": rec2.get("instanceType", ""),
            "vCPU II": rec2.get("vCPU", ""),
            "Monthly Price II": rec2.get("monthlyCost", ""),
            "Annual Cost II": rec2.get("totalCost", ""),
            "Annual Savings II": rec2.get("annualSavings", ""),
            "Savings % II": rec2.get("savingsInPercentage", ""),
            "Perf Enhancement II": rec2.get("perf", ""),
            "Zone II": rec2.get("zone", ""),

            # Recommendation III
            "Recommendation III Instance": rec3.get("instanceType", ""),
            "vCPU III": rec3.get("vCPU", ""),
            "Monthly Price III": rec3.get("monthlyCost", ""),
            "Annual Cost III": rec3.get("totalCost", ""),
            "Annual Savings III": rec3.get("annualSavings", ""),
            "Savings % III": rec3.get("savingsInPercentage", ""),
            "Perf Enhancement III": rec3.get("perf", ""),
            "Zone III": rec3.get("zone", ""),

            "comments": rec.get("comments", ""),
            "created_at": datetime.utcnow()
        })

    if records_to_insert:
        # ✅ Bulk insert in one call
        result = await recommended_instance_collection.insert_many(records_to_insert)
        log_message(
            LevelType.INFO,
            f"Inserted {len(result.inserted_ids)} recommendations where app : {app_name}",
            portfolio_id=portfolio_id
        )
    else:
        log_message(
            LevelType.INFO,
            f"No recommendations to insert for portfolio_id={portfolio_id} where app : {app_name}",
            portfolio_id=portfolio_id
        )

    log_message(
        LevelType.INFO,
        f"Finished storing {len(records_to_insert)} recommendations for portfolio_id={portfolio_id} where app : {app_name}",
        portfolio_id=portfolio_id
    )
        

async def store_eia_recommendations(
    transformed_data,
    portfolio_id,
    recommended_instance_collection,
    app_name="EIA"
):
    if isinstance(transformed_data, dict):
        transformed_data = [transformed_data]

    # ✅ Delete old records in one call
    delete_result = await recommended_instance_collection.delete_many({"portfolio_id": portfolio_id})
    log_message(
        LevelType.INFO,
        f"Deleted {delete_result.deleted_count} old recommendations where app: {app_name}",
        portfolio_id=portfolio_id
    )

    # ✅ Pre-build all records in memory
    records_to_insert = []
    for rec in transformed_data:
        current = rec.get("data", {}).get("currentPlatform", {})
        recommendations = rec.get("data", {}).get("recommendations", [])

        rec1 = recommendations[0] if len(recommendations) > 0 else {}
        rec2 = recommendations[1] if len(recommendations) > 1 else {}

        records_to_insert.append({
            "portfolio_id": str(portfolio_id),
            "UUID": rec.get("id", ""),
            "CSP": rec.get("csp", "").upper(),
            "Pricing Model": current.get("pricingModel", ""),
            "Zone": current.get("region", ""),
            "Current Instance": current.get("type", ""),
            "vCPU": current.get("vCPU", ""),
            "Current Monthly Price": current.get("cost", ""),
            "Current Instance Energy Consumption (kwh)": current.get("power", ""),
            "Current Instance Emission": current.get("carbon", ""),

            # Recommendation I
            "Recommendation I Instance": rec1.get("type", ""),
            "vCPU I": rec1.get("vCPU", ""),
            "Monthly Price I": rec1.get("cost", ""),
            "Monthly Savings I": rec1.get("monthlySavings", ""),
            "Instance Energy Consumption I (kwh)": rec1.get("power", ""),
            "Instance Emission I": rec1.get("carbon", ""),
            "Perf Enhancement I": rec1.get("perf", ""),
            "Untapped Capacity I": rec1.get("untappedCapacity", ""),

            # Recommendation II
            "Recommendation II Instance": rec2.get("type", ""),
            "vCPU II": rec2.get("vCPU", ""),
            "Monthly Price II": rec2.get("cost", ""),
            "Monthly Savings II": rec2.get("monthlySavings", ""),
            "Instance Energy Consumption II (kwh)": rec2.get("power", ""),
            "Instance Emission II": rec2.get("carbon", ""),
            "Perf Enhancement II": rec2.get("perf", ""),
            "Untapped Capacity II": rec2.get("untappedCapacity", ""),

            "STATUS": current.get("status", ""),
            "comments": rec.get("comments", ""),
            "created_at": datetime.utcnow()
        })

    if records_to_insert:
        # ✅ Bulk insert in a single DB call
        result = await recommended_instance_collection.insert_many(records_to_insert)
        log_message(
            LevelType.INFO,
            f"Inserted {len(result.inserted_ids)} recommendations where app: {app_name}",
            portfolio_id=portfolio_id
        )
    else:
        log_message(
            LevelType.INFO,
            f"No recommendations to insert for app: {app_name}",
            portfolio_id=portfolio_id
        )

    log_message(
        LevelType.INFO,
        f"Finished storing {len(records_to_insert)} recommendations where app: {app_name}",
        portfolio_id=portfolio_id
    )


def _validate_headroom(headroom: int):
    if headroom < 0:
        log_message(LevelType.ERROR, "Headroom value must be a positive number", ErrorCode=-1)
        raise CustomAPIException(status_code=400, message="Headroom value must be a positive number", error_code=-1)

async def process_last_cost_advise(user_mail: str, app_name: str, portfolio_id: str, page: int, page_size: int, is_chart_value: Optional[bool] = False):
    COST_ADVICE_MSG = ""
    try:
        if not ObjectId.is_valid(portfolio_id):  # format: 24-char hex or 12-byte input
            raise CustomAPIException(
                status_code=400,
                message=f"Invalid portfolio_id '{portfolio_id}'."
            ) 
        # Get collections
        portfolio_collection = get_collection(CollectionNames.PORTFOLIOS)
        recommended_instance_collection = get_collection(CollectionNames.RECOMMENDED_INSTANCES)

        portfolio_doc = await portfolio_collection.find_one({"_id": ObjectId(portfolio_id), "app_name": app_name})
        if not portfolio_doc:
            log_message(LevelType.ERROR, f"Portfolio not found id : {portfolio_id} and app_name : {app_name}", ErrorCode=-1, portfolio_id=portfolio_id)
            raise CustomAPIException(status_code=404, message=f"Portfolio not found id : {portfolio_id} and app_name : {app_name}", error_code=-1)
        
        advice_s3_key = portfolio_doc.get("advice_s3_key")
        ppt_s3_key = portfolio_doc.get("ppt_s3_key")
        invalid_record_s3_key = portfolio_doc.get("invalid_record_s3_key")
        
        if portfolio_doc.get("recommendation_status") != RecommendationStatus.COMPLETED:
            log_message(LevelType.ERROR, "Recommendations not available", ErrorCode=-1, portfolio_id=portfolio_id)
            raise CustomAPIException(status_code=404, message="Recommendations not available", error_code=-1)
        
        # Extra ownership check
        if portfolio_doc.get("user_email") != user_mail:
            log_message(
                LevelType.ERROR,
                f"Unauthorized access: portfolio belongs to {portfolio_doc.get('user_email')}, not {user_mail}",
                ErrorCode=-1,
                portfolio_id=portfolio_id,
            )
            raise CustomAPIException(
                status_code=403,
                message="You are not authorized to access this portfolio.",
                error_code=-1,
            )

        file_name = f"{portfolio_doc.get('name')}_{portfolio_id}"

        pagination_result = await paginate_collection(
            recommended_instance_collection,
            {"portfolio_id": portfolio_id},
            page=page,
            limit=page_size,
        )

        reform_data, grand_total, chart_value = await reformat_recommendation_data(pagination_result["items"], app_name, portfolio_id, recommended_instance_collection, is_chart_value)
        
        advice_url , ppt_url, invalid_record_url = "", "", ""
        if advice_s3_key:
            advice_url = generate_download_presigned_url(advice_s3_key)
        if ppt_s3_key:
            ppt_url = generate_download_presigned_url(ppt_s3_key)
        if invalid_record_s3_key:
            invalid_record_url = generate_download_presigned_url(invalid_record_s3_key)
        
        response =  {
                    "ErrorCode": 1,
                    "Message": "Recommendation successfully fetched.",
                    "current_page": pagination_result["current_page"],
                    "page_size": pagination_result["page_size"],
                    "total_pages": pagination_result["total_pages"],
                    "total_items" : pagination_result["total_items"]
                    }
        response.update(reform_data)
        response["grandTotal"] = grand_total
        if chart_value:
            response["chart_values"] = chart_value
        if pagination_result.get("total_items"):
            response.update({"fileName": file_name + ".xlsx",
                    "ExportPath": advice_url, 
                    "Invalid Record" : invalid_record_url}) 
            if app_name.upper() == "CCA":
                response["PPT File Path"] = ppt_url
            elif app_name.upper() == "EIA":
                response["PPT File Path"] = ppt_url

        return response

    except CustomAPIException:
        raise
    except Exception as e:
        log_message(LevelType.ERROR, f"{str(e)}, Details: {COST_ADVICE_MSG}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message="Failed to fetch recommendation data", error_code=-1)


async def process_cost_advise(db: Session, user_mail: str, app_name: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    udf_file_path = None
    policy_engine_file_path = None
    COST_ADVICE_MSG = ""
    try:
        portfolio_id = payload.get("portfolioId")
        headroom = payload.get("headroom%", 20)
        is_refetch = payload.get("is_refetch_recommendation", False)

        page = payload.get("page", 1) 
        page_size = payload.get("page_size", 10)

        # Get collections
        portfolio_collection = get_collection(CollectionNames.PORTFOLIOS)
        current_instance_collection = get_collection(CollectionNames.CURRENT_INSTANCES)
        recommended_instance_collection = get_collection(CollectionNames.RECOMMENDED_INSTANCES)

        request = get_request()
        if request:
            request.state.portfolio_id = portfolio_id

        if app_name.strip().upper() not in ["EIA", "CCA"]:
            log_message(LevelType.ERROR, "Invalid App Name.", ErrorCode=-1, portfolio_id=portfolio_id)
            raise CustomAPIException(status_code=400, message="Invalid App Name.", error_code=-1)
        _validate_headroom(headroom)
        
        portfolio_doc = await portfolio_collection.find_one({"_id": ObjectId(portfolio_id), "app_name": app_name.upper()})
        if not portfolio_doc:
            log_message(LevelType.ERROR, "Portfolio not found", ErrorCode=-1, portfolio_id=portfolio_id)
            raise CustomAPIException(status_code=404, message="Portfolio not found", error_code=-1)
        
        # Extra ownership check
        if portfolio_doc.get("user_email") != user_mail:
            log_message(
                LevelType.ERROR,
                f"Unauthorized access: portfolio belongs to {portfolio_doc.get('user_email')}, not {user_mail}",
                ErrorCode=-1,
                portfolio_id=portfolio_id,
            )
            raise CustomAPIException(
                status_code=403,
                message="You are not authorized to access this portfolio.",
                error_code=-1,
            )
        transformed_data = {}
        file_name = f"{portfolio_doc.get('name')}_{portfolio_id}"
        csv_file_name = f"{file_name}.csv"
        ppt_name= ""
        if is_refetch:
            udf_data = portfolio_doc.get("udf_path")

            portfolio_collection.update_one(
                {"_id": ObjectId(portfolio_id)},
                {
                    "$set": {
                        "headroom": headroom,
                        "submittedForRecommendations": True
                    }
                },
                upsert=False  # don't insert new, update only
            )
            
            # Fetch related instances from current_instance
            instance_cursor = current_instance_collection.find({"portfolio_id": portfolio_id})
            instances = await instance_cursor.to_list(length=None)  # Get all
            
            input_folder_path = os.path.join(base_path, 'input')
            output_folder_path = os.path.join(base_path, 'output')
            

            if app_name.upper() == 'EIA':
                udf_folder_path = os.path.join(base_path, 'udf')
                udf_file_name = f"{portfolio_id}_udf.{'csv'}"
                udf_file_path = os.path.join(udf_folder_path, udf_file_name)
                input_file_path = os.path.join(input_folder_path, csv_file_name)

                output_csv_file_path = os.path.join(output_folder_path, csv_file_name)
                input_csv_file_path, udf_file_path = create_instance_udf_files_from_json(instances, headroom, udf_data, input_file_path, udf_file_path)

            else:
                policy_data = []
                default_policy = "No Policy Engine (Default)"
                policy_engine = portfolio_doc.get("policy_engine")
                if policy_engine:
                    if policy_engine.lower() != default_policy.lower():
                        rows = (
                            db.query(
                                PolicyEngine.instance_type,
                                PolicyEngine.scalar_value,
                            )
                            .filter(
                                func.lower(PolicyEngine.provider) == portfolio_doc.get("cloud_provider").lower(),
                                func.lower(PolicyEngine.policy_name) == portfolio_doc.get("policy_engine").lower(),
                                or_(PolicyEngine.user_email == user_mail, PolicyEngine.user_email == "")
                            )
                            .order_by(PolicyEngine.id)
                            .all()
                        )
                        policy_data = [
                            {"instance_type": r.instance_type, "scalar_value": r.scalar_value}
                            for r in rows
                        ]
                policy_engine_folder_path = os.path.join(base_path, 'udf')
                policy_engine_file_name = f"{portfolio_id}_udf.{'csv'}"
                policy_engine_file_path = os.path.join(policy_engine_folder_path, policy_engine_file_name)
                input_csv_file_path, output_csv_file_path, policy_engine_file_path = costadvise_utils(instances, input_folder_path, output_folder_path, csv_file_name, policy_data, policy_engine_file_path)
                
            COST_ADVICE_MSG, result, flag = data_collection(input_csv_file_path, output_csv_file_path, udf_file_path, policy_engine_file_path, app_name)

            if not flag:
                if os.path.exists(input_csv_file_path):
                    os.remove(input_csv_file_path)
                log_message(LevelType.ERROR, "Unable to find recommendation data.", ErrorCode=-1, portfolio_id=portfolio_id)
                portfolio_collection.update_one(
                    {"_id": ObjectId(portfolio_id)},
                    {"$set": {"status": "Failed"}}
                )
                raise CustomAPIException(status_code=400, message="Unable to find recommendation data",data={"Details": COST_ADVICE_MSG}, error_code=-1)
            
            transformed_data = get_transformed_rec_data(app_name, result)
            
            if app_name.upper() == 'CCA':
                generate_excel_from_json({"data": transformed_data, "grandTotal": result["grandTotal"]}, output_csv_file_path.replace('.csv', '.xlsx'))
            elif app_name.upper() == 'EIA':
                generate_excel_report(transformed_data, result["grandTotal"], output_csv_file_path.replace('.csv', '.xlsx'), headroom)
            

        else:
            pagination_result = await paginate_collection(
                recommended_instance_collection,
                {"portfolio_id": portfolio_id},
                page=page,
                limit=page_size,
            )
            reform_data, grand_total, chart_value = await reformat_recommendation_data(pagination_result["items"], app_name, portfolio_id, recommended_instance_collection, is_chart_value=True)

        if not transformed_data and is_refetch:
            if os.path.exists(input_csv_file_path) and os.path.exists(output_csv_file_path):
                os.remove(input_csv_file_path)
                os.remove(output_csv_file_path)
                xlsx_file_name = output_csv_file_path.replace('.csv', '.xlsx')
                if os.path.exists(xlsx_file_name):
                    os.remove(xlsx_file_name)
            if udf_file_path and os.path.exists(udf_file_path):
                os.remove(udf_file_path)

            if policy_engine_file_path:
                if os.path.exists(policy_engine_file_path):
                    os.remove(policy_engine_file_path)
            log_message(LevelType.ERROR, f"Unable to transform the data, Details: {COST_ADVICE_MSG}", ErrorCode=-1, portfolio_id=portfolio_id)
            portfolio_collection.update_one(
                {"_id": ObjectId(portfolio_id)},
                {"$set": {"status": "Failed"}}
            )
            raise CustomAPIException(status_code=400, message="Unable to transform the data, Details.", data={"Details": COST_ADVICE_MSG}, error_code=-1)

        elif transformed_data and is_refetch:
            if isinstance(transformed_data, dict):
                transformed_data = [transformed_data]

            # Sort by currentPlatform.instanceType
            transformed_data.sort(
                key=lambda x: x.get("data", {}).get("currentPlatform", {}).get("instanceType", "")
            )
            xl_file = output_csv_file_path.replace('.csv', '.xlsx')
            ppt_path, ppt_name  = "", ""
            if app_name == "CCA":
                ppt_file = os.path.join(base_path, "AMD_EPYC_PPT_TEMPLATE.pptx")
                if os.path.exists(output_csv_file_path):
                    if os.path.exists(ppt_file):
                        ppt_name = generate_ppt(xl_file, user_mail, ppt_file, output_folder_path)
                        ppt_path = os.path.join(output_folder_path, ppt_name)
            
            elif app_name == "EIA":
                ppt_template = os.path.join(base_path, "EIA_Presentation_Screen.pptx")
                if os.path.exists(xl_file) and os.path.exists(ppt_template):
                    ppt_name = f"{file_name}_EIA.pptx"
                    ppt_path = os.path.join(output_folder_path, ppt_name)
                    try:
                        generate_ppt_from_excel(xl_file, ppt_template, ppt_path, user_mail)
                        log_message(LevelType.INFO, f"EIA PPT generated: {ppt_path}", ErrorCode=1, portfolio_id=portfolio_id)
                    except Exception as e:
                        log_message(LevelType.ERROR, f"Failed to generate EIA PPT: {str(e)}", ErrorCode=-1, portfolio_id=portfolio_id)
                        ppt_name = ""
                        ppt_path = ""

            excel_s3_key, ppt_s3_key = await upload_generated_files_to_s3(file_name, xl_file, ppt_name, ppt_path, app_name, user_mail)
            advice_url = generate_download_presigned_url(excel_s3_key) if excel_s3_key else ""
            ppt_url = generate_download_presigned_url(ppt_s3_key) if ppt_s3_key else ""
            
            log_message(LevelType.INFO, f"inputFile: {csv_file_name}, outputFile: {csv_file_name}, Recommendation successfully fetched., Details : {COST_ADVICE_MSG}", ErrorCode=1,  portfolio_id=portfolio_id)
            portfolio_collection.update_one(
                {"_id": ObjectId(portfolio_id)},
                {"$set": {"status": "Passed", "ppt_s3_key" : ppt_s3_key, "advice_s3_key" : excel_s3_key}}
            )
            log_message(LevelType.INFO, f"length of transformed_data : {len(transformed_data)} where app : {app_name}", portfolio_id=portfolio_id)
            portfolio_doc = await portfolio_collection.find_one({"_id": ObjectId(portfolio_id)})
            portfolio_headroom_value = portfolio_doc.get("headroom")
            if app_name.upper() == 'EIA':
                await store_eia_recommendations(
                    transformed_data=transformed_data,
                    portfolio_id=portfolio_id,
                    recommended_instance_collection=recommended_instance_collection
                )
                log_message(LevelType.INFO, "Recommendation successfully saved for EIA.", ErrorCode=1, portfolio_id=portfolio_id)
                headroom_value = get_input_headroom(result)
                if os.path.exists(input_csv_file_path) and os.path.exists(output_csv_file_path):
                    os.remove(input_csv_file_path)
                    os.remove(output_csv_file_path)
                    xlsx_file_name = output_csv_file_path.replace('.csv', '.xlsx')
                    if os.path.exists(xlsx_file_name):
                        os.remove(xlsx_file_name)
                    if os.path.exists(ppt_path):
                        os.remove(ppt_path)
                if udf_file_path and os.path.exists(udf_file_path):
                        os.remove(udf_file_path)

                paginated_result = paginate_transformed_data(transformed_data, page=page, page_size=page_size)
                asyncio.create_task(etl_process_for_portfolio(portfolio_id))
                instance_cursor = recommended_instance_collection.find({"portfolio_id": portfolio_id})
                data_list = await instance_cursor.to_list(length=None)
                chart_value = energy_chart_eval_from_flat(data_list)
                return {
                    "ErrorCode": 1,
                    "Message": "Recommendation successfully fetched.",
                    "Data": paginated_result["Data"],
                    "headroom%": portfolio_headroom_value,
                    "grandTotal": result['grandTotal'],
                    "fileName": file_name + ".xlsx",
                    "ExportPath": advice_url,
                    "PPT File Path": ppt_url,
                    "Details": COST_ADVICE_MSG,
                    "current_page": paginated_result["current_page"],
                    "page_size": paginated_result["page_size"],
                    "total_pages": paginated_result["total_pages"],
                    "total_items" : len(transformed_data),
                    "chart_values" : chart_value
                }
            else:
                await store_cca_recommendations(
                    transformed_data=transformed_data,
                    portfolio_id=portfolio_id,
                    recommended_instance_collection=recommended_instance_collection
                )
                log_message(LevelType.INFO, "Recommendation successfully saved for CCA.", ErrorCode=1, portfolio_id=portfolio_id)
                if os.path.exists(input_csv_file_path) and os.path.exists(output_csv_file_path):
                    os.remove(input_csv_file_path)
                    os.remove(output_csv_file_path)
                    xlsx_file_name = output_csv_file_path.replace('.csv', '.xlsx')
                    if os.path.exists(xlsx_file_name):
                        os.remove(xlsx_file_name)
                    if policy_engine_file_path:
                        if os.path.exists(policy_engine_file_path):
                            os.remove(policy_engine_file_path)
                chart_data = dollar_spend_eval_from_json(transformed_data, result['grandTotal'])
                paginated_result = paginate_transformed_data(transformed_data, page=page, page_size=page_size)
                asyncio.create_task(etl_process_for_portfolio(portfolio_id))
                return {
                "ErrorCode": 1,
                "Message": "Recommendation successfully fetched.",
                "Data": paginated_result["Data"],
                "headroom%": portfolio_headroom_value,
                "grandTotal": result['grandTotal'],
                "fileName": file_name + ".xlsx",
                "ExportPath": advice_url,
                "Details": COST_ADVICE_MSG,
                "PPT File Path": ppt_url,
                "current_page": paginated_result["current_page"],
                "page_size": paginated_result["page_size"],
                "total_pages": paginated_result["total_pages"],
                "total_items" : len(transformed_data),
                "chart_values" : chart_data

                }
        else:
            headroom_value = portfolio_doc.get("headroom")
            ppt_s3_key = portfolio_doc.get("ppt_s3_key", "")
            advice_s3_key = portfolio_doc.get("advice_s3_key", "")
            advice_url = generate_download_presigned_url(advice_s3_key) if advice_s3_key else ""

            response =  {
                        "ErrorCode": 1,
                        "Message": "Recommendation successfully fetched.",
                        "headroom%": headroom_value,
                        "current_page": pagination_result["current_page"],
                        "page_size": pagination_result["page_size"],
                        "total_pages": pagination_result["total_pages"],
                        "total_items" : pagination_result["total_items"]
                        }
            response.update(reform_data)
            response["grandTotal"] = grand_total
            if chart_value:
                response["chart_values"] = chart_value
            if pagination_result.get("total_items"):
                response.update({"fileName": file_name + ".xlsx",
                        "ExportPath": advice_url})
                if app_name.upper() == "CCA":
                    ppt_url = generate_download_presigned_url(ppt_s3_key) if ppt_s3_key else ""
                    response["PPT File Path"] = ppt_url
                elif app_name.upper() == "EIA":
                    ppt_url = generate_download_presigned_url(ppt_s3_key) if ppt_s3_key else ""
                    response["PPT File Path"] = ppt_url
            
            return response
    except CustomAPIException:
        raise

    except Exception as e:
        log_message(LevelType.ERROR, f"{str(e)}, Details: {COST_ADVICE_MSG}", ErrorCode=-1)
        portfolio_collection.update_one(
            {"_id": ObjectId(portfolio_id)},
            {"$set": {"status": "Failed"}}
        )
        raise CustomAPIException(status_code=500, message="Failed to fetch recommendation data", error_code=-1)


async def process_get_recommendations(user_mail: str, app_name: str, payload: Dict[str, Any], ipaddr, db: Session) -> Dict[str, Any]:
    COST_ADVICE_MSG = ""
    udf_file_path = None
    policy_engine_file_path = None
    portfolio_id = ""
    try:
        portfolio_name = payload.get("portfolioName")
        provider = payload.get("provider")
        policy_engine = payload.get("policy_engine").strip().lower() if payload.get("policy_engine") else None
        input_data = payload.get("data")
        headroom = payload.get("headroom%", 20)
        udf_data = payload.get("udf", [])
        is_downloadable = payload.get("downloadable_link", False)
        password = payload.get("password") or portfolio_name
        request = get_request()

        
        portfolio_collection = get_collection(CollectionNames.PORTFOLIOS)
        recommended_instance_collection = get_collection(CollectionNames.RECOMMENDED_INSTANCES)

        _validate_headroom(headroom)
        
        input_folder_path = os.path.join(base_path, 'input')
        output_folder_path = os.path.join(base_path, 'output')

        if app_name.upper() == AppName.CCA:
            instance_data, _, message, _ = await validate_input_data(input_data, provider, app_name, udf_data, request)
        if app_name.upper() == AppName.EIA:
            instance_data, _ , _, _ = await validate_input_data(input_data, provider.upper(),  app_name, udf_data, request)

        if not instance_data:
            log_message(LevelType.ERROR, f"Validation failed: {message}", ErrorCode=-1)
            raise CustomAPIException(status_code=400, message=message, error_code=-1)

        for instance in instance_data:
            if instance.get("Remarks"):  # non-empty list evaluates to True
                log_message(LevelType.ERROR, "Invalid input data.",data=instance_data,ErrorCode=-1)
                if app_name.upper() == AppName.CCA:
                    raise CustomAPIException(status_code=400, message='Invalid input data.', data={"Data" : instance_data})
                else:
                    raise CustomAPIException(status_code=400, message='Invalid input data.', data={"Data" : instance_data})
        
        input_data = instance_data
        
        existing = await portfolio_collection.find_one({
            "name": portfolio_name,
            "user_email": user_mail,
            "cloud_provider": provider,
            "app_name": app_name.upper(),
        })
        if existing:
            portfolio_id = str(existing["_id"])
            await patch_portfolio_data(
                update_fields={
                    "portfolioName": portfolio_name,
                    "provider": provider,
                    "policy_engine": policy_engine,
                    "headroom": headroom,
                    "data": input_data
                },
                _id=portfolio_id,
                app_name=app_name,
                db=db,
                user_email=user_mail
            )
        else:
            # 1. Save portfolio & instances
            payload_dict: Dict[str, Any] = {
                "portfolioName": portfolio_name,
                "provider": provider,
                "user_email": user_mail,
                "headroom%": headroom,
                "data": input_data,
                "appName" : app_name,
                "udf" : udf_data
            }
            if app_name.upper() == "CCA" and policy_engine:
                payload_dict["policy_engine"]=policy_engine
            request_model = TypeAdapter(SavePortfolioRequest).validate_python(payload_dict)
            save_result = await save_portfolio_data(db, request_model, app_name, ipaddr or None, user_mail)
            portfolio_id = save_result.get("portfolio_id")

        if request:
            request.state.portfolio_id = portfolio_id

        file_name = f"{portfolio_name}_{portfolio_id}"
        csv_file_name = f"{file_name}.csv"


        if app_name.upper() == 'EIA':
            udf_folder_path = os.path.join(base_path, 'udf')
            udf_file_name = f"{portfolio_id}_udf.{'csv'}"
            udf_file_path = os.path.join(udf_folder_path, udf_file_name)
            input_file_path = os.path.join(input_folder_path, csv_file_name)

            output_csv_file_path = os.path.join(output_folder_path, csv_file_name)
            input_csv_file_path, udf_file_path = create_instance_udf_files_from_json(input_data, headroom, udf_data, input_file_path, udf_file_path)

        else:
            policy_data = []
            default_policy = "No Policy Engine (Default)"
            if policy_engine:
                if policy_engine.lower() != default_policy.lower():
                    rows = (
                        db.query(
                            PolicyEngine.instance_type,
                            PolicyEngine.scalar_value
                        )
                        .filter(
                            func.lower(PolicyEngine.provider) == provider.lower(),
                            func.lower(PolicyEngine.policy_name) == policy_engine.lower(),
                            or_(PolicyEngine.user_email == user_mail, PolicyEngine.user_email == ""),
                        )
                        .order_by(PolicyEngine.id)
                        .all()
                    )

                    policy_data = [
                        {"instance_type": r.instance_type, "scalar_value": r.scalar_value}
                        for r in rows
                    ]
            policy_engine_folder_path = os.path.join(base_path, 'udf')
            policy_engine_file_name = f"{portfolio_id}_udf.{'csv'}"
            policy_engine_file_path = os.path.join(policy_engine_folder_path, policy_engine_file_name)
            input_csv_file_path, output_csv_file_path, policy_engine_file_path = costadvise_utils(input_data, input_folder_path, output_folder_path, csv_file_name, policy_data, policy_engine_file_path)
        
        portfolio_collection.update_one(
            {"_id": ObjectId(portfolio_id)},
            {"$set": {"submittedForRecommendations": True}}
        )
        

        COST_ADVICE_MSG, result, flag = data_collection(input_csv_file_path, output_csv_file_path, udf_file_path, policy_engine_file_path, app_name)

        if not flag:
            if os.path.exists(input_csv_file_path):
                os.remove(input_csv_file_path)
            log_message(LevelType.ERROR, "Unable to find recommendation data.", ErrorCode=-1, portfolio_id=portfolio_id)
            portfolio_collection.update_one(
                {"_id": ObjectId(portfolio_id)},
                {"$set": {"status": "Failed"}}
            )
            raise CustomAPIException(status_code=400, message="Unable to find recommendation data",data={"Details": COST_ADVICE_MSG}, error_code=-1)

        transformed_data = get_transformed_rec_data(app_name, result)
        if not transformed_data:
            if os.path.exists(input_csv_file_path) and os.path.exists(output_csv_file_path):
                os.remove(input_csv_file_path)
                os.remove(output_csv_file_path)
                xlsx_file_name = output_csv_file_path.replace('.csv', '.xlsx')
                if os.path.exists(xlsx_file_name):
                    os.remove(xlsx_file_name)
            log_message(LevelType.ERROR, "Unable to transform the data.", ErrorCode=-1, portfolio_id=portfolio_id)
            portfolio_collection.update_one(
                {"_id": ObjectId(portfolio_id)},
                {"$set": {"status": "Failed"}}
            )
            raise CustomAPIException(status_code=400, message="Unable to transform the data",data={"Details": COST_ADVICE_MSG}, error_code=-1)

        if app_name.upper() == 'CCA':
            generate_excel_from_json({"data": transformed_data, "grandTotal": result["grandTotal"]}, output_csv_file_path.replace('.csv', '.xlsx'))
        elif app_name.upper() == 'EIA':
            generate_excel_report(transformed_data, result["grandTotal"], output_csv_file_path.replace('.csv', '.xlsx'), headroom)
        
        if isinstance(transformed_data, dict):
            transformed_data = [transformed_data]

        # Sort by currentPlatform.instanceType
        transformed_data.sort(
            key=lambda x: x.get("data", {}).get("currentPlatform", {}).get("instanceType", "")
        )
        
        ppt_name = ""
        if app_name == AppName.CCA and is_downloadable:
            xl_file = output_csv_file_path.replace(".csv", ".xlsx")
            ppt_file = os.path.join(base_path, "AMD_EPYC_PPT_TEMPLATE.pptx")
            if os.path.exists(output_csv_file_path):
                if os.path.exists(ppt_file):
                    ppt_name = generate_ppt(xl_file, user_mail, ppt_file, output_folder_path, password)
                    protect_existing_excel(f"{output_folder_path}/{file_name}.xlsx", password)
                    log_message(LevelType.INFO, f"password is protected for file : {output_folder_path}{file_name}.xlsx", ErrorCode=1, portfolio_id=portfolio_id)
        
        if app_name == AppName.EIA and is_downloadable:
            xl_file = output_csv_file_path.replace(".csv", ".xlsx")
            ppt_template = os.path.join(base_path, "EIA_Presentation_Screen.pptx")
            if os.path.exists(xl_file) and os.path.exists(ppt_template):
                ppt_name = f"{file_name}_EIA.pptx"
                ppt_output_path = os.path.join(output_folder_path, ppt_name)
                try:
                    generate_ppt_from_excel(xl_file, ppt_template, ppt_output_path, user_mail, password)
                    log_message(LevelType.INFO, f"EIA PPT generated: {ppt_output_path}", ErrorCode=1, portfolio_id=portfolio_id)
                except Exception as e:
                    log_message(LevelType.ERROR, f"Failed to generate EIA PPT: {str(e)}", ErrorCode=-1, portfolio_id=portfolio_id)
                    ppt_name = ""  # Reset if generation failed
            protect_existing_excel(f"{output_folder_path}/{file_name}.xlsx", password)
            log_message(LevelType.INFO, f"password is protected for file : {output_folder_path}{file_name}.xlsx", ErrorCode=1, portfolio_id=portfolio_id)

        ppt_path = ""
        excel_path = os.path.join(output_folder_path, f"{file_name}.xlsx")
        if ppt_name:
            ppt_path = os.path.join(output_folder_path, ppt_name)
        else:
            log_message(LevelType.INFO, "No ppt", ErrorCode=-1)
        excel_s3_key, ppt_s3_key = "", ""
        if is_downloadable:
            excel_s3_key, ppt_s3_key = await upload_generated_files_to_s3(file_name, excel_path, ppt_name, ppt_path, app_name, user_mail)
            advice_url = generate_download_presigned_url(excel_s3_key) if excel_s3_key else ""
            ppt_url = generate_download_presigned_url(ppt_s3_key) if ppt_s3_key else ""
        
        os.remove(excel_path)
        if ppt_path:
            os.remove(ppt_path)

        log_message(LevelType.INFO, f"inputFile: {csv_file_name}, outputFile: {csv_file_name}, Recommendation successfully fetched.", ErrorCode=1, portfolio_id=portfolio_id)
        portfolio_collection.update_one(
            {"_id": ObjectId(portfolio_id)},
            {"$set": {"status": "Passed", "ppt_s3_key" : ppt_s3_key, "advice_s3_key" : excel_s3_key}}
        )
        log_message(LevelType.INFO, f"length of transformed_data : {len(transformed_data)} where app : {app_name}", portfolio_id=portfolio_id)
        if app_name.upper() == AppName.EIA:
                await store_eia_recommendations(
                    transformed_data=transformed_data,
                    portfolio_id=portfolio_id,
                    recommended_instance_collection=recommended_instance_collection
                )
                log_message(LevelType.INFO, "Recommendation successfully saved for EIA.", ErrorCode=1, portfolio_id=portfolio_id)

        else:
            await store_cca_recommendations(
                    transformed_data=transformed_data,
                    portfolio_id=portfolio_id,
                    recommended_instance_collection=recommended_instance_collection
                )
            log_message(LevelType.INFO, "Recommendation successfully saved for CCA.", ErrorCode=1, portfolio_id=portfolio_id)
            
        if os.path.exists(input_csv_file_path) and os.path.exists(output_csv_file_path):
            os.remove(input_csv_file_path)
            os.remove(output_csv_file_path)
            xlsx_file_name = output_csv_file_path.replace('.csv', '.xlsx')
            if os.path.exists(xlsx_file_name):
                os.remove(xlsx_file_name)
        
        response = {
            "ErrorCode": 1,
            "Message": "Recommendation successfully fetched.",
            "Data": transformed_data,
            "headroom%": headroom,
            "grandTotal": result.get("grandTotal")
        }
        if app_name.upper() == AppName.CCA and is_downloadable:
            response["fileName"] = f"{file_name}.xlsx"
            response["ExportPath"] = advice_url
            response["PPT File Path"] = ppt_url
        if app_name.upper() == AppName.EIA and is_downloadable:
            response["fileName"] = f"{file_name}.xlsx"
            response["ExportPath"] = advice_url
            response["PPT File Path"] = ppt_url
        asyncio.create_task(etl_process_for_portfolio(portfolio_id))
        return response
    except CustomAPIException:
        raise
    except Exception as e:
        log_message(LevelType.ERROR, {str(e)}, ErrorCode=-1)
        if portfolio_id:
            portfolio_collection.update_one(
                {"_id": ObjectId(portfolio_id)},
                {"$set": {"status": "Failed"}}
            )
        raise CustomAPIException(status_code=500, message="Failed to fetch recommendation data", error_code=-1)

async def etl_process_for_portfolio(portfolio_id):
    portfolio_collection = get_collection(CollectionNames.PORTFOLIOS)
    recommendation_collection = get_collection(CollectionNames.RECOMMENDED_INSTANCES)
    analytics_collection = get_collection(CollectionNames.RECOMMENDATION_ANALYTICS)
    unsupported_recommendation_collection = get_collection(CollectionNames.ANALYTICS_WITHOUT_RECOMMENDATION)

    doc = await portfolio_collection.find_one(
        {"_id": ObjectId(portfolio_id)},
        {"user_email": 1, "cloud_provider": 1, "created_at": 1, "updated_at": 1, "app_name": 1, "name": 1}
    )

    if not doc:
        log_message(LevelType.WARNING, f"Portfolio {portfolio_id} not found for ETL process.", portfolio_id=portfolio_id)
        return False

    app_name = doc.get("app_name", "").upper()
    user, org = extract_org_and_user_from_email(doc.get("user_email", ""))
    created_at_utc = convert_to_utc(doc.get("created_at"))
    portfolio_name = doc.get("name", "")

    # Single aggregation pipeline that includes costs, savings, counts, vCPUs, and performance
    pipeline = get_eia_pipeline(portfolio_id) if app_name == "EIA" else get_cca_pipeline(portfolio_id)

    cursor = recommendation_collection.aggregate(pipeline)
    aggregate_result = await cursor.to_list(length=1)

    if aggregate_result:
        
        aggregate_result = aggregate_result[0]

        if app_name == "EIA":
            h_vcpus = aggregate_result.get("HC_VCPUs", 0)
            m_vcpus = aggregate_result.get("M_VCPUs", 0)
            total_vcpus_avg = (h_vcpus + m_vcpus) / 2 if (h_vcpus + m_vcpus) > 0 else 0
            total_perf_avg = (aggregate_result.get("H_Perf", 0) + aggregate_result.get("M_Perf", 0)) / 2

            final_result = {
                "org": org,
                "app_name": doc.get("app_name"),
                "user_email": doc.get("user_email"),
                "portfolio_id": portfolio_id,
                "portfolio_name": portfolio_name,
                "user": user,
                "cloud": doc.get("cloud_provider"),
                "portfolio_created_date": created_at_utc,
                "recommendation_date": aggregate_result.get("recommendation_date", None),
                "current_vCPUs": aggregate_result.get("Current_Vcpus", 0),
                "O_vCPUs": h_vcpus,
                "G_vCPUs": m_vcpus,
                "total_vCPUs": total_vcpus_avg,
                "O_perf": aggregate_result.get("H_Perf", 0),
                "G_perf": aggregate_result.get("M_Perf", 0),
                "total_perf": total_perf_avg,
                "O_saving": aggregate_result.get("H_Saving", 0),
                "G_saving": aggregate_result.get("M_Saving", 0),
                "O_carboncount": aggregate_result.get("H_carboncount", 0),
                "G_carboncount": aggregate_result.get("M_carboncount", 0),
                "O_energycount": aggregate_result.get("H_energycount", 0),
                "G_energycount": aggregate_result.get("M_energycount", 0),
                "M_D_perf": 0,
                "M_D_saving": 0,
                
                # Cost, savings, and count fields from aggregation
                "current_cost": aggregate_result.get("current_cost", 0),
                "O_cost": aggregate_result.get("O_cost", 0),
                "G_cost": aggregate_result.get("G_cost", 0),
                "recommendation_instances_count": aggregate_result.get("recommendation_instances_count", 0),
                "recommendation_regions_count": aggregate_result.get("recommendation_regions_count", 0),
            }
        else:
            h_vcpus = aggregate_result.get("HC_VCPUs", 0)
            m_vcpus = aggregate_result.get("M_VCPUs", 0)
            md_vcpus = aggregate_result.get("MD_VCPUs", 0)
            total_vcpus_avg = (h_vcpus + m_vcpus + md_vcpus) / 3 if (h_vcpus + m_vcpus + md_vcpus) > 0 else 0
            total_perf_avg = (
                aggregate_result.get("H_Perf", 0) +
                aggregate_result.get("M_Perf", 0) +
                aggregate_result.get("MD_Perf", 0)
            ) / 3

            final_result = {
                "org": org,
                "app_name": doc.get("app_name"),
                "user_email": doc.get("user_email"),
                "portfolio_id": portfolio_id,
                "portfolio_name": portfolio_name,
                "user": user,
                "cloud": doc.get("cloud_provider"),
                "portfolio_created_date": created_at_utc,
                "recommendation_date": aggregate_result.get("recommendation_date", None),
                "current_vCPUs": aggregate_result.get("Current_Vcpus", 0),
                "HC_vCPUs": h_vcpus,
                "M_vCPUs": m_vcpus,
                "M_D_vCPUs": md_vcpus,
                "total_vCPUs": total_vcpus_avg,
                "HC_perf": aggregate_result.get("H_Perf", 0),
                "M_perf": aggregate_result.get("M_Perf", 0),
                "M_D_perf": aggregate_result.get("MD_Perf", 0),
                "total_perf": total_perf_avg,
                "HC_saving": aggregate_result.get("H_Saving", 0),
                "M_saving": aggregate_result.get("M_Saving", 0),
                "M_D_saving": aggregate_result.get("MD_Saving", 0),

                # Cost, savings, and count fields from aggregation
                "current_cost": aggregate_result.get("current_cost", 0),
                "HC_cost": aggregate_result.get("HC_cost", 0),
                "M_cost": aggregate_result.get("M_cost", 0),
                "M_D_cost": aggregate_result.get("M_D_cost", 0),
                "recommendation_instances_count": aggregate_result.get("recommendation_instances_count", 0),
                "recommendation_regions_count": aggregate_result.get("recommendation_regions_count", 0),
            }

        final_result["created_at"] = datetime.utcnow()

        # Insert new analytics entry (keeping historical entries)
        await analytics_collection.insert_one(final_result)
    else:
        log_message(LevelType.WARNING, f"No recommendation data found for portfolio {portfolio_id} during ETL.", portfolio_id=portfolio_id)
    unsupported_instances = await get_unsupported_instances(portfolio_id, app_name, user)
    if unsupported_instances:
        unsupported_recommendation_collection.insert_many(unsupported_instances)
    log_message(LevelType.INFO, f"ETL process completed for portfolio {portfolio_id}", portfolio_id=portfolio_id)
    return True

async def get_unsupported_instances(portfolio_id, app_name, user):
    condition = None
    recommendation_collection = get_collection(CollectionNames.RECOMMENDED_INSTANCES)
    
    # Specific logic per app:
    # EIA: Check I and II
    # CCA/Others: Check I, II, and III
    if app_name.upper() == "EIA":
        condition = {
            "$and": [
                {"$eq": ["$Recommendation I Instance", "-"]},
                {"$eq": ["$Recommendation II Instance", "-"]}
            ]
        }
    else:
        condition = {
            "$and": [
                {"$eq": ["$Recommendation I Instance", "-"]},
                {"$eq": ["$Recommendation II Instance", "-"]},
                {"$eq": ["$Recommendation III Instance", "-"]}
            ]
        }

    pipeline = [
        {"$match": {"portfolio_id": portfolio_id}},
        {"$project": {
            "CSP": 1,
            "Zone": 1,
            "Current Instance": 1,
            "created_at": 1,
            "Recommendation I Instance": 1,
            "Recommendation II Instance": 1,
            "Recommendation III Instance": 1,
        }},
        {"$addFields": {
            "matching_recs": condition
        }},
        {"$match": {"matching_recs": True}}
    ]

    etl_cursor = recommendation_collection.aggregate(pipeline)
    docs = await etl_cursor.to_list(length=None)
    result = []
    for rec in docs:
        created_at = rec.get("created_at")
        result.append({
            "portfolio_id": portfolio_id,
            "cloud": rec.get("CSP"),
            "region": rec.get("Zone"),
            "current_instance": rec.get("Current Instance"),
            "created_at": created_at,
            "app_name": app_name,
            "user": user
        })
    return result


async def upload_generated_files_to_s3(file_name: str,excel_path :str, ppt_name: str, ppt_path : str,  app_name: str, user_email: str, sub_folder: str = "output"):
    """
    Upload Excel and PPT to S3 if they exist, then delete them locally.
    Returns a dict containing S3 keys.
    """

    excel_s3_key = None
    ppt_s3_key = None

    # --- 1️⃣ Excel Upload ---
    if os.path.exists(excel_path):
        try:
            with open(excel_path, "rb") as f:
                excel_bytes = f.read()

            excel_s3_key = await upload_file_to_s3(
                file_bytes=excel_bytes,
                app_name=app_name,
                user_email=user_email,
                file_name=f"{file_name}.xlsx",
                sub_folder=sub_folder
            )

            log_message(LevelType.INFO, f"Excel uploaded to S3: {excel_s3_key}", ErrorCode=1)

        except Exception as e:
            log_message(LevelType.ERROR, f"Excel upload error: {e}", ErrorCode=-1)
    else:
        log_message(LevelType.ERROR, f"Excel file not found in {excel_path}", ErrorCode=-1)


    if os.path.exists(ppt_path):
        try:
            with open(ppt_path, "rb") as f:
                ppt_bytes = f.read()

            ppt_s3_key = await upload_file_to_s3(
                file_bytes=ppt_bytes,
                app_name=app_name,
                user_email=user_email,
                file_name=ppt_name,
                sub_folder=sub_folder
            )

            log_message(LevelType.INFO, f"PPT uploaded to S3: {ppt_s3_key}", ErrorCode=1)

        except Exception as e:
            log_message(LevelType.ERROR, f"PPT upload error: {e}", ErrorCode=-1)
    else:
        log_message(LevelType.ERROR, f"ppt file not found in {ppt_path}", ErrorCode=-1)

    return excel_s3_key, ppt_s3_key


def to_float_safe(v):
    """
    Convert Monthly Price I value to float if possible;
    return None for '-', '', None, invalid.
    """
    if v in ("", "-", None):
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def is_valid_zone(z):
    """Check if a zone value is meaningful."""
    return z not in (None, "", "-")


def build_portfolio_query(raw_pid):
    """
    Build a flexible query for portfolio_id:
    - As exact string
    - As trimmed string
    - As ObjectId (if the string is a valid ObjectId)
    This helps if portfolio_id types differ between collections.
    """
    clauses = []
    if raw_pid is None:
        return {"portfolio_id": None}

    if isinstance(raw_pid, str):
        clauses.append({"portfolio_id": raw_pid})
        trimmed = raw_pid.strip()
        if trimmed != raw_pid:
            clauses.append({"portfolio_id": trimmed})
        try:
            from bson.errors import InvalidId
            oid = ObjectId(trimmed)
            clauses.append({"portfolio_id": oid})
        except (InvalidId, Exception):
            pass
    else:
        # already non-string, maybe ObjectId
        clauses.append({"portfolio_id": raw_pid})

    if len(clauses) == 1:
        return clauses[0]
    return {"$or": clauses}


async def get_instance_and_region_counts(recommendation_collection, raw_pid, app_name):
    """
    Count recommendation instances and unique regions for a portfolio.

    - recommendation_instances_count: Count of records where Monthly Price I > 0
    - recommendation_regions_count: Count of unique Zone values where Monthly Price I > 0
      - For CCA: uses "Zone I" field
      - For EIA: uses "Zone" field

    Returns: (instances_count, regions_count)
    """
    query = build_portfolio_query(raw_pid)

    instances_count = 0
    regions = set()

    # Determine which zone field to use based on app type
    zone_field = "Zone I" if app_name == "CCA" else "Zone"

    inst_cursor = recommendation_collection.find(
        query,
        {
            "Monthly Price I": 1,
            zone_field: 1,
        }
    )

    docs = await inst_cursor.to_list(length=None)

    for inst in docs:
        price_I = to_float_safe(inst.get("Monthly Price I"))
        zone = inst.get(zone_field)

        # Count instances where Monthly Price I > 0
        if price_I is not None and price_I > 0:
            instances_count += 1

            # Also track unique regions for these valid instances
            if is_valid_zone(zone):
                regions.add(zone)

    return instances_count, len(regions)