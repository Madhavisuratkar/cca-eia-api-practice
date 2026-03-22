import asyncio
from unittest.mock import AsyncMock, MagicMock
from app.utils.common_utils import calculate_energy_grand_total

async def test_calculate_energy_grand_total():
    # Mock collection and cursor
    mock_collection = MagicMock()
    mock_cursor = AsyncMock()
    
    # Sample data with Untapped Capacity
    sample_data = [
        {
            "Zone": "Zone 1",
            "Current Monthly Price": 100,
            "Current Instance Energy Consumption (kwh)": 50,
            "Current Instance Emission": 10,
            "Monthly Price I": 80,
            "Instance Energy Consumption I (kwh)": 40,
            "Instance Emission I": 8,
            "Monthly Savings I": 20,
            "Perf Enhancement I": 10,
            "Untapped Capacity I": 20, # Val 1
            "Monthly Price II": 70,
            "Instance Energy Consumption II (kwh)": 35,
            "Instance Emission II": 7,
            "Monthly Savings II": 30,
            "Perf Enhancement II": 15,
            "Untapped Capacity II": 25 # Val 1
        },
        {
            "Zone": "Zone 1",
            "Current Monthly Price": 120,
            "Current Instance Energy Consumption (kwh)": 60,
            "Current Instance Emission": 12,
            "Monthly Price I": 90,
            "Instance Energy Consumption I (kwh)": 45,
            "Instance Emission I": 9,
            "Monthly Savings I": 30,
            "Perf Enhancement I": 0, # Should be ignored in average
            "Untapped Capacity I": 30, # Val 2
            "Monthly Price II": 75,
            "Instance Energy Consumption II (kwh)": 38,
            "Instance Emission II": 8,
            "Monthly Savings II": 45,
            "Perf Enhancement II": 25,
            "Untapped Capacity II": 0 # Should be ignored in average
        }
    ]
    
    # Setup mock return
    mock_collection.find.return_value = mock_cursor
    mock_cursor.to_list.return_value = sample_data
    
    # Run function
    grand_total, chart_value = await calculate_energy_grand_total("dummy_portfolio_id", mock_collection)
    
    print("Grand Total:", grand_total)
    
    # Assertions
    # Untapped Capacity I: (20 + 30) / 2 = 25.0
    assert grand_total["Untapped Capacity I"] == 25.0, f"Expected 25.0 for Untapped Capacity I, got {grand_total['Untapped Capacity I']}"
    
    # Untapped Capacity II: (25) / 1 = 25.0 (since 0 is ignored)
    assert grand_total["Untapped Capacity II"] == 25.0, f"Expected 25.0 for Untapped Capacity II, got {grand_total['Untapped Capacity II']}"
    
    print("Verification Passed!")

if __name__ == "__main__":
    asyncio.run(test_calculate_energy_grand_total())
