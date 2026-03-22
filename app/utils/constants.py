"""here keeping all constat values"""

class ApplicationEndpoints:
    """application endpoints"""
    ROOT = "/"
    DOCS = "/docs"
    SWAGGER_JSON = "/swagger.json"
    OPEN_AI = "/open-ai"
    HEALTH_CHECK = "/health/{app_name}"
    OPEN_AI_JSON = "/openapi.json"
    SWAGGER_UI = "/swagger-ui/<path:filename>"
    STATIC = "/static/<path:filename>"
    SAVINGS = "/savings"
    MATRICS = "/metrics"
    ORGANIZATION = "/organizations"
    FEATURES_COUNT = "/features/count"
    JENKINS_DATA = "/jenkins/{job_name}"
    SONAR_DATA = "/sonar"
    PORTFOLIOS = "/portfolios"
    SAVE_PORTFOLIO = "/save-portfolio"
    GET_PORTFOLIO = "/get-portfolio"
    UPDATE_PORTFOLIO = "/update-portfolio"
    DELETE_PORTFOLIO = "/delete-portfolio"
    PORTFOLIO_WITH_CRED_ACCOUNT = "/cloud-accounts"
    PORTFOLIO_WITH_CRED = "/cloud-accounts/{query_type}"
    GET_REGIONS = "/regions"
    GET_INSTANCE_SIZES = "/instance-sizes"
    EXPLORER = "/explorer"
    INSTANCE_SUMMARY = "/instances/summary"
    GET_CLOUD_INSTANCES = "/cloud-instances"
    FILE_UPLOAD_VALIDATE = "/file-upload/validate"
    INPUT_VALIDATE = "/input/validate"
    INPUT_CORRECT = "/input/correct"
    TELEMETRY_CONNECTION = "/telemetry/connection/{source_type}"
    TELEMETRY_METRICS = "/telemetry/metrics/{source_type}"
    COST_ADVISE="/cost-advice"
    RECOMMENDATIONS="/recommendations"
    LAST_RECOMMENDATIONS="/latest-recommendations"
    LOGIN = "/login"
    DELETE_INSTANCE = "/delete-instance"
    GENERATE_UPLOAD_URL = '/upload-portfolio'
    GET_EXCEL_ROWCOUNT = "/get-excel-rowcount"
    PORTFOLIO_COST_ADVICE = "/initiate-costAdvice"
    JOB_STATUS = "/job-status"
    NOTIFICATIONS ="/notifications"
    NOTIFICATIONS_READ ="/notifications/read"
    NOTIFICATIONS_DEL ="/notifications/del"
    DASHBOARD = "/dashboard"
    LIST_PORTFOLIOS = "/listportfolios"
    LOCK_UNLOCK_PORTFOLIO = "/lockUnlockPortfolio"
    SALES_CLIENT_LIST = "/sales-client/list"
    SALES_CLIENT_ADD = "/sales-client/add"
    SALES_CLIENT_DELETE = "/sales-client/delete"
    SALES_CLIENT_FAVORITE = "/sales-client/favorite"
    INSIGHTS = "/insights"
    UNIQUE_PORTFOLIO_USERS = "/unique-portfolio-users"
    DASHBOARD_ANALYTICS = "/dashboard_analytics"
    DASHBOARD_CLIENT_SUMMARY = "/dashboard_client_summary"


class ApplicationModuleTag:
    PORTFOLIO_WITHOUT_CLOUD_CONTROLLERS = "Portfolio without Cloud Cred"
    PORTFOLIO_WITH_CLOUD_CONTROLLERS = "Portfolio with Cloud Cred"
    EXPLORER = "Cloud Explorer"
    INPUT_VALIDATION = "Input validation"
    TELEMETRY_CONTROLLERS = "Telemetry data extraction"
    COST_ADVICE = "Cost advise"
    ETL_OPERATIONS = "ETL operations"
    LAST_RECOMMENDATIONS = "Last Recommendations"

class LevelType:
    """logger levels"""
    INFO = "info"
    ERROR = "error"
    WARNING = "warning"
    DEBUG = "debug"

class AppName:
    """current app names"""
    EIA = 'EIA'
    CCA = 'CCA'

class UserRoles:
    USER = "user"

api_user_access = {
    '_cost_advise': [[AppName.EIA, AppName.CCA], [UserRoles.USER]],
    '_get_regions': [[AppName.EIA, AppName.CCA], [UserRoles.USER]],
    '_get_instance_sizes': [[AppName.EIA, AppName.CCA], [UserRoles.USER]],
    '_get_explorer': [[AppName.EIA, AppName.CCA], [UserRoles.USER]],
    '_instances_summary': [[AppName.EIA, AppName.CCA], [UserRoles.USER]],
    '_get_cloud_sizes': [[AppName.EIA, AppName.CCA], [UserRoles.USER]],
    '_login': [[AppName.EIA, AppName.CCA], [UserRoles.USER]],
    '_test_cloud_connection': [[AppName.CCA], [UserRoles.USER]],
    '_add_cloud_account': [[AppName.CCA], [UserRoles.USER]],
    '_sync_cloud_account': [[AppName.CCA], [UserRoles.USER]],
    '_get_cloud_account': [[AppName.EIA, AppName.CCA], [UserRoles.USER]],
    '_list_portfolio': [[AppName.EIA, AppName.CCA], [UserRoles.USER]],
    '_list_all_portfolios': [[AppName.EIA, AppName.CCA], [UserRoles.USER]],
    '_delete_portfolio': [[AppName.EIA, AppName.CCA], [UserRoles.USER]],
    '_rename_portfolio': [[AppName.EIA, AppName.CCA], [UserRoles.USER]],
    '_save_portfolio': [[AppName.EIA, AppName.CCA], [UserRoles.USER]],
    '_file_upload_validate': [[AppName.EIA, AppName.CCA], [UserRoles.USER]],
    '_input_validate': [[AppName.EIA, AppName.CCA], [UserRoles.USER]],
    '_input_correct': [[AppName.EIA, AppName.CCA], [UserRoles.USER]],
    '_test_datadog_connection': [[AppName.EIA, AppName.CCA], [UserRoles.USER]],
    '_get_datadog_instance_metrics': [[AppName.EIA, AppName.CCA], [UserRoles.USER]],
    '_features_count':[[AppName.EIA, AppName.CCA], [UserRoles.USER]],
    '_metrics':[[AppName.EIA, AppName.CCA], [UserRoles.USER]],
    '_savings':[[AppName.EIA, AppName.CCA], [UserRoles.USER]],
    '_organizations':[[AppName.EIA, AppName.CCA],[UserRoles.USER]]
}

field_mappings = {
        AppName.CCA: ['uuid', 'cloud_csp', 'instance type', 'monthly utilization (hourly)', 'pricingModel', 'quantity', 'region'],
        AppName.EIA: ['cloud_csp', 'instance type', 'region', 'uuid', 'max cpu%', 'max mem used', 'max network bw', 'max disk bw used', 'max iops', 'pricingModel', 'pavg', 'uavg', 'p95', 'u95']
    }

CLOUD_PROVIDERS = ["AWS", "AZURE", "GCP", "OCI","DATADOG", "CLOUDWATCH", "AZUREINSIGHTS", "GCPTELEMETRY", "PROMETHEUS"]
UNSUPPORTED_PROVIDERS = ["OCI"]

ALLOWED_PROVIDERS = ["AWS", "AZURE", "GCP"]

LOG_FILE = 'logger.log'
FILE_EXTENSION = '.json'
MONTHLY_UTILIZATION = "monthly utilization (hourly)"
UNKNOWN_IP = "unknown ip"
UNKNOWN_USER = "unknown user"
UNKNOWN_APP = "unknown app"
REQUERED_FIELD_ERROR = "All required inputs are not given"
FAILED_INSTANCE = "Failed to fetch instances data"
FAILED_REGION = "Failed to fetch regions data"
ERROR_AMD_INSTANCE = "Unable to get AMD instances"
USERDATA_ERROR = "User data not found"
INSTANCE_TYPE = 'instance type'

DB_PREFIX = "postgresql+psycopg2"
NEGATIVE_REPLACEMENT_TEXT = "EIA Recommended"

TOKEN_URI = "https://oauth2.googleapis.com/token"
UNIVERSE_URI = "googleapis.com"
CCA_APP = "CCA"
EIA_APP = "EIA"
ACTION = "read"
COSTADVICE_MSG = "Failed to fetch recommendation data"
PRICING_DATABASE = 'pricing_database.h5'
CLOUD_DATABASE = 'cloud_database.h5'
EXPLORER_FILE = "output/explore.csv"
CCA_DEFICIENT_FILE = "input/demo.csv"
USER_VALIDATION_STR = 'User validation Token'
PORTFOLIO_MSG = "Portfolio not found"
PORTFOLIO_SUCCESS_MSG = 'Portfolio fetched successfully'
JSON_STR = '.json'
CLOUD_MSG = 'Cloud provider'
CSV_STR = '.csv'
XL_STR = '.xlsx'
RESULTS_PATH = '/home/ubuntu/cloudsolutions-ui/dist/results'
INSTANCE = INSTANCE_TYPE
UTILIZATION = "monthly utilization (hourly)"
USER_ERROR = "Unauthorized user."
APP_FLAG = 'Name of the application'
METADATA_ERROR = "Metadata not found"
NUMBER_VALIDATION = "must be a positive number"
INVALID_APP = 'Invalid application provided'
PIPE = "|"

CLOUD_PROVIDERS = ["AWS", "AZURE", "GCP", "OCI","DATADOG", "CLOUDWATCH", "AZUREINSIGHTS", "GCPTELEMETRY", "PROMETHEUS"]
UNSUPPORTED_PROVIDERS = ["OCI"]
VALID_APPS = ["CCA", "EIA"]
PRICING_MODEL = ['ondemand', 'reserved', 'spot']
UNSUPPORTED_PRICING_MODEL = [""]
AMAZON_WEB_SERVICES = "Amazon Web Services"
HOST = 'host:'
VERIFY_SIGNATURE = "verify_signature"
ETL_APP="ETL"
ENDPOINT_TABLE_NAME='endpoints'
PRICEMODEL = 'pricing model'

vCPU = 'vcpu'
vCPU_i = 'vcpu i'
vCPU_ii = 'vcpu ii'
vCPU_iii = 'vcpu iii'

VCPU='C'
VCPU_I='HC'
VCPU_II='M'
VCPU_III='M&D'

awsAccessId = 'AWS Access Key ID'
awsAccessSecret = 'AWS Access Secret'
awsRegion = 'AWS Region'
azureClientId = 'AZURE Client ID'
azureClientSecret = 'AZURE Client Secret'
azureTenantId = 'AZURE Tenant ID'
azureSubscriptionId = 'AZURE Subscription ID'
gcpProjectId = 'GCP Project Id'
gcpPrivateKey = 'GCP Private Key'
gcpClientEmail = 'GCP Client Email'
gcpClientId = 'GCP Client Id'

DATA_SUCCESS = "Data validated successfully"
INSTANCE_ERROR = "Instance type is required"
MAX_CPU = 'max cpu%'
MAX_MEM_USED = "max mem used"
MAX_NW_BW = "max network bw"
MAX_DISK_BW = "max disk bw used"
MAX_IOPS = "max iops"
REGION_REQUIRED = "Region is required"
HOURS_REQUIRED = "Hours is required"
NO_DATA_IN_FILE = "No data available in provided file"
FILE_VALIDATE_ERROR = "Provider input is invalid"
INSTANCE_NAME = "instance name"

DATADOG_URL="https://api.datadoghq.com"
DATADOG_REGION = "region:"
DATADOG_NAME = "name:"

REGEX_NAME = '$regex'
OPTIONS_NAME='$options'
INVALID_DAYS_MSG="Invalid days filter"
APP_HEADER_MSG="Appname header missing"
DATE_FORMATTER="%Y-%m-%d"
MATCH_NAME="$match"
SORT_NAME="$sort"
GROUP_NAME="$group"
PROJECT_NAME = "$project"
INACTIVE_USERS = "Inactive Users"
FEATURES_MSG="Feature data fetched successfully"
IS_NULL="$ifNull"

ANNUAL_COST = "Annual Cost"
ANNUAL_SAVINGS_I = "Annual Savings I"
ANNUAL_SAVINGS_II = "Annual Savings II"
ANNUAL_SAVINGS_III = "Annual Savings III"
PERF_ENHANCEMENT_I = 'Perf Enhancement I'
PERF_ENHANCEMENT_II = 'Perf Enhancement II'
PERF_ENHANCEMENT_III = 'Perf Enhancement III'
PRICE_MODEL='pricingModel'
HEADROOM = 'headroom%'

AWS_MAX_CPU = "aws.ec2.cpuutilization.maximum"
MEM_USED = "system.mem.used"
AWS_MAX_NET_IN = "aws.ec2.network_in.maximum"
AWS_MAX_NET_OUT = "aws.ec2.network_out.maximum"
AWS_EBS_READ = "aws.ec2.ebsread_ops.sum"
AWS_READ_WRITE = "aws.ec2.ebswrite_ops.sum"
AWS_DISK_READ = "aws.ec2.disk_read_ops"
AWS_DISK_WRITE = "aws.ec2.disk_write_ops"
AWS_EBS_READ_BYTES = "aws.ec2.ebsread_bytes.sum"
AWS_EBS_WRITE_BYTES = "aws.ec2.ebswrite_bytes.sum"
AWS_DISK_READ_BYTES = "aws.ec2.disk_read_bytes"
AWS_DISK_WRITE_BYTES = "aws.ec2.disk_write_bytes"
AZURE_CPU = 'azure.vm.percentage_cpu'
AZURE_NET_IN = 'azure.vm.network_in_total'
AZURE_NET_OUT = 'azure.vm.network_out_total'
AZURE_DISK_READ = 'azure.vm.disk_read_operations_sec'
AZURE_DISK_WRITE = 'azure.vm.disk_write_operations_sec'
AZURE_DISK_READ_BYTES = 'azure.vm.disk_read_bytes'
AZURE_DISK_WRITE_BYTES = 'azure.vm.disk_write_bytes'
GCP_CPU = 'gcp.gce.instance.cpu.utilization'
GCP_NET_REC = 'gcp.gce.instance.network.received_bytes_count'
GCP_NET_SENT = 'gcp.gce.instance.network.sent_bytes_count'
GCP_DISK_READ = 'gcp.gce.instance.disk.read_ops_count'
GCP_DISK_WRITE = 'gcp.gce.instance.disk.write_ops_count'
GCP_DISK_READ_BYTE = 'gcp.gce.instance.disk.read_bytes_count'
GCP_DISK_WRITE_BYTE = 'gcp.gce.instance.disk.write_bytes_count'

DAYS_LOOKBACK = 1
PERIOD = 300
NAMESPACE_EC2 = 'AWS/EC2'
NAMESPACE_CWAGENT = 'CWAgent'

DAYS_BACK = 1
METRIC_NAMESPACE = "Microsoft.Compute/virtualMachines"
MAX_WORKERS = 10

NAMESPACE_GCP = "compute.googleapis.com"
NAMESPACE_AGENT = "agent.googleapis.com"

METRIC_MAP = {
    "Percentage CPU": "max_cpu_percent",
    "Network In Total": "net_in",
    "Network Out Total": "net_out",
    "Disk Read Operations/Sec": "disk_read_ops",
    "Disk Write Operations/Sec": "disk_write_ops",
    "Disk Read Bytes": "disk_read_bytes",
    "Disk Write Bytes": "disk_write_bytes",
}

modernize_down_monthly = 'modernize & downsize monthly cost'
no_of_instances = 'number of instances'
skipped_instance = 'skipped instance'

monthly_price_i = 'monthly price i'
monthly_price_ii = 'monthly price ii'
monthly_price_iii = 'monthly price iii'

current_monthly_price = 'current monthly price'
current_instance_energy_consumption = 'current instance energy consumption (kwh)'
current_instance_emission = 'current instance emission'
monthly_utilization = 'monthly utilization'

db_connection_msg = "Database connection failed"

TABLE_SCHEMA_QUERY = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"


eia_input_column_rename_dict = {
    'instance_type': INSTANCE_TYPE,
    'INSTANCE type': INSTANCE_TYPE,
    'Host': 'hostname',
    'host': 'hostname',
    'max_cpu': MAX_CPU,
    'max_mem_used': MAX_MEM_USED,
    'max mem used (gib)': MAX_MEM_USED,
    'max_network_bw': MAX_NW_BW,
    'max_disk_bw_used': MAX_DISK_BW,
    'max_iops': MAX_IOPS
}

eia_output_column_rename_dict = {
    f'{current_monthly_price} ($)': current_monthly_price,
    f'{current_instance_emission} (kgco2eq)': current_instance_emission,
    f'{monthly_price_i} ($)': monthly_price_i,
    'instance emission i (kgco2eq)': 'instance emission i',
    f'{monthly_price_ii} ($)': monthly_price_ii,
    'instance emission ii (kgco2eq)': 'instance emission ii',
    f'{monthly_price_iii} ($)': monthly_price_iii,
    'instance emission iii (kgco2eq)': 'instance emission iii'
}

eia_output_numeric_columns = [
    current_monthly_price, monthly_price_i, monthly_price_ii, monthly_price_iii,
    current_instance_energy_consumption, 'instance energy consumption i (kwh)',
    'instance energy consumption ii (kwh)', 'instance energy consumption iii (kwh)',
    current_instance_emission, 'instance emission i', 'instance emission ii', 'instance emission iii'
]

cca_output_column_rename_dict = {
    'recommendation i instance': 'hourly cost optimization instance',
    'monthly cost i': 'hourly cost optimization monthly cost',
    'annual cost i': 'hourly cost optimization annual cost',
    'annual savings i': 'hourly cost optimization total savings',
    'recommendation ii instance': 'modernize instance',
    'monthly cost ii': 'modernize monthly cost',
    'annual cost ii': 'modernize annual cost',
    'annual savings ii': 'modernize total savings',
    'recommendation iii instance': 'modernize & downsize instance',
    'monthly cost iii (perf scaled)': modernize_down_monthly,
    'annual cost iii': 'modernize & downsize annual cost',
    'annual savings iii': 'modernize & downsize total savings',
}

cca_output_numeric_columns = [
    'annual cost', 'current monthly cost', 'hourly cost optimization monthly cost',
    'hourly cost optimization annual cost', 'hourly cost optimization total savings',
    'modernize monthly cost', 'modernize annual cost', 'modernize total savings',
    modernize_down_monthly, 'modernize & downsize annual cost', 'modernize & downsize total savings'
]
TOKEN_URI = "https://oauth2.googleapis.com/token"
UNIVERSE_URI = "googleapis.com"

ENV_FILE_PATH = '/etc/cca_eia_secrets.env'
DEFAULT_ENV_PATH = '.env'

TARGET_SCHEMA = [
    "UUID",
    "Cloud",
    "Region",
    "Size",
    "Quantity",
    "Total number of hours per month",
    "Pricing Model",
]
 
REQUIRED_FOR_REGEX = {"Cloud", "Pricing Model", "Region", "Size"}

LARGE_KEYS = {"customer_name", "cloud_provider", "Date_Format"}  # 14 pt

class CollectionNames:
    PORTFOLIOS = "portfolios"
    CURRENT_INSTANCES = "current_instance"
    RECOMMENDED_INSTANCES= "recommended_instances"
    ENDPOINTS = "endpoints"
    HEALTH_CHECK = "health_check"
    RECOMMENDATION_TRACKING = "recommendation_tracking"
    NOTIFICATIONS = "notifications"
    
    # ✅ New summary collections
    ORG_USER_SUMMARY = "org_user_summary"
    ORG_ENDPOINT_SUMMARY = "org_endpoint_summary"

    RECOMMENDATION_ANALYTICS = "recommendation_analytics"
    ANALYTICS_WITHOUT_RECOMMENDATION = "recommendation_unsupported_analytics"

    SALES_CLIENT = "sales_client"
    ORGANIZATION_DATA = "organization_data"


ENDPOINT_DOCS = {
    ("GET", ApplicationEndpoints.SAVINGS): {
        "success": "Fetched savings data successfully",
        "summary": "Get Savings Data",
        "description": "Retrieve savings data for the authenticated application."
    },
    ("GET", ApplicationEndpoints.MATRICS): {
        "success": "Fetched metrics successfully",
        "summary": "Get Metrics",
        "description": "Retrieve system and performance metrics."
    },
    ("GET", ApplicationEndpoints.ORGANIZATION): {
        "success": "Fetched organizations successfully",
        "summary": "Get Organizations",
        "description": "Retrieve list of organizations for the authenticated application."
    },
    ("GET", ApplicationEndpoints.FEATURES_COUNT): {
        "success": "Fetched features count successfully",
        "summary": "Get Features Count",
        "description": "Retrieve counts of enabled features."
    },
    ("GET", ApplicationEndpoints.JENKINS_DATA): {
        "success": "Fetched Jenkins job data successfully",
        "summary": "Get Jenkins Job Data",
        "description": "Retrieve last build and status information for the specified Jenkins job."
    },
    ("GET", ApplicationEndpoints.SONAR_DATA): {
        "success": "Fetched Sonar analysis data successfully",
        "summary": "Get Sonar Data",
        "description": "Retrieve SonarQube analysis data for projects."
    },
    ("GET", ApplicationEndpoints.PORTFOLIOS): {
        "success": "Fetched portfolios successfully",
        "summary": "Get Portfolios",
        "description": "Retrieve a list of portfolios."
    },
    ("POST", ApplicationEndpoints.PORTFOLIOS): {
        "success": "Portfolio created successfully",
        "summary": "Create Portfolio",
        "description": "Create a new portfolio entry."
    },
    ("GET", ApplicationEndpoints.GET_REGIONS): {
        "success": "Fetched available cloud regions successfully",
        "summary": "Get Cloud Regions",
        "description": "Fetch available regions for a given cloud provider."
    },
    ("GET", ApplicationEndpoints.GET_INSTANCE_SIZES): {
        "success": "Fetched available instance sizes successfully",
        "summary": "Get Instance Sizes",
        "description": "Fetch available instance sizes for a given provider and region."
    },
    ("GET", ApplicationEndpoints.EXPLORER): {
        "success": "Fetched explorer AMD instances successfully",
        "summary": "Explore Instances",
        "description": "Fetch AMD instance information from data store."
    },
    ("GET", ApplicationEndpoints.INSTANCE_SUMMARY): {
        "success": "Fetched instance summary successfully",
        "summary": "Get Instance Summary",
        "description": "Retrieve cloud regions and instances summary."
    },
    ("GET", ApplicationEndpoints.GET_CLOUD_INSTANCES): {
        "success": "Fetched cloud instances successfully",
        "summary": "Get Cloud Instances",
        "description": "Retrieve available cloud instances for a provider."
    },
    ("POST", ApplicationEndpoints.FILE_UPLOAD_VALIDATE): {
        "success": "File upload validation successful",
        "summary": "Validate File Upload",
        "description": "Validate user-uploaded files for structure and content."
    },
    ("POST", ApplicationEndpoints.INPUT_VALIDATE): {
        "success": "Input validation successful",
        "summary": "Validate Input",
        "description": "Validate input data before processing."
    },
    ("POST", ApplicationEndpoints.INPUT_CORRECT): {
        "success": "Input correction successful",
        "summary": "Correct Input",
        "description": "Automatically correct invalid or incomplete input fields."
    },
    ("GET", ApplicationEndpoints.COST_ADVISE): {
        "success": "Fetched cost advice successfully",
        "summary": "Get Cost Advice",
        "description": "Retrieve cloud cost optimization advice."
    },
    ("GET", ApplicationEndpoints.RECOMMENDATIONS): {
        "success": "Fetched recommendations successfully",
        "summary": "Get Recommendations",
        "description": "Retrieve AI-driven recommendations for your infrastructure."
    },
    ("POST", ApplicationEndpoints.LOGIN): {
        "success": "Login successful",
        "summary": "User Login",
        "description": "Authenticate user and obtain access token."
    },
    ("GET", ApplicationEndpoints.HEALTH_CHECK): {
        "success": "Healthcheck successful",
        "summary": "Health Check",
        "description": "Check the service health and basic request info like client IP, method, and user-agent."
    },
    ("PATCH", ApplicationEndpoints.PORTFOLIOS): {
        "success": "Portfolio renamed successfully",
        "summary": "Rename Portfolio",
        "description": "Rename an existing portfolio by specifying its ID."
    },
    ("DELETE", ApplicationEndpoints.PORTFOLIOS): {
        "success": "Portfolio deleted successfully",
        "summary": "Delete Portfolio",
        "description": "Delete a specific portfolio by its ID."
    },
    ("POST", ApplicationEndpoints.PORTFOLIO_WITH_CRED): {
        "success": "Cloud account added successfully",
        "summary": "Add Cloud Account",
        "description": "Add a new cloud account to a portfolio for the authenticated user."
    },
    ("GET", ApplicationEndpoints.PORTFOLIO_WITH_CRED): {
        "success": "Fetched cloud account details successfully",
        "summary": "Get/Sync/Test Cloud Account",
        "description": "Perform cloud account operations: get_account, sync_account, or test_account for a user's portfolio."
    },
    ("POST", ApplicationEndpoints.TELEMETRY_CONNECTION): {
        "success": "Telemetry connection tested successfully",
        "summary": "Test Telemetry Connection",
        "description": "Test the telemetry data connection for the provided application context."
    },
    ("POST", ApplicationEndpoints.TELEMETRY_METRICS): {
        "success": "Telemetry metrics fetched successfully",
        "summary": "Get Telemetry Metrics",
        "description": "Retrieve telemetry metrics for instances, including headroom percentage and data details."
    },
    ("POST", ApplicationEndpoints.COST_ADVISE): {
        "success": "Fetched cost advice successfully",
        "summary": "Get Cost Advice",
        "description": "Process and retrieve cloud cost optimization advice for the given user and application."
    },
    ("POST", ApplicationEndpoints.RECOMMENDATIONS): {
        "success": "Fetched recommendations successfully",
        "summary": "Get Recommendations",
        "description": "Retrieve AI-driven infrastructure optimization recommendations based on cost and usage data."
    }
}

class RecommendationStatus:
    TO_PROCESS = "TO_PROCESS"
    QUEUE = "QUEUE"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


REQUIRED_HEADERS = {
    AppName.EIA:  ["cloud_csp", INSTANCE_TYPE, "region", MAX_CPU, MAX_MEM_USED, MAX_NW_BW, MAX_DISK_BW, MAX_IOPS, 'uavg', 'u95', PRICEMODEL],
    AppName.CCA: ["cloud", "region", "size", "quantity", "total number of hours per month", PRICEMODEL]
}

MAX_EXCEL_ROW_LIMIT = 1_000_000  # maximum allowed rows per sheet
