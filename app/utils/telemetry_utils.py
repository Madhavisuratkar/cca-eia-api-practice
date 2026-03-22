import numpy as np
from google.cloud import compute_v1

def get_average(cpu_values):
    uavg = round(np.mean(cpu_values), 2) if cpu_values else 0
    u95 = round(np.percentile(cpu_values, 95), 2) if cpu_values else 0
    pavg = uavg
    p95 = u95
    return uavg, pavg, u95, p95

def replace_spaces_with_plus(key_pem):
    if "\\n" in key_pem:
        key_pem = key_pem.replace("\\n", "\n")
    key_pem = key_pem.replace(" ", "+")
    lines = key_pem.splitlines()
    return "\n".join(line.replace("+", " ") if "PRIVATE+KEY" in line else line for line in lines)

def list_reservations(reservations_client, project, zone):
    """List all Compute Engine reservations for a zone"""
    request = compute_v1.ListReservationsRequest(
        project=project,
        zone=zone
    )
    return reservations_client.list(request)

def is_instance_reserved(instance, reservations):
    """Check if instance is covered by a reservation"""
    instance_type = instance.machine_type.split("/")[-1]
    for reservation in reservations:
        # Check if any specific reservation covers this instance type
        for specific_reservation in reservation.specific_reservations:
            if instance_type in [vm.machine_type for vm in specific_reservation.instance_properties]:
                return True
    return False
