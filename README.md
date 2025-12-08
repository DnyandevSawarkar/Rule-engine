# Rule Engine Library v1.7.0

Advanced rule engine library for processing airline coupon data with **direct DataFrame support**, comprehensive row-exploded output, UTC/IST timestamps, addon rule support, and robust formula parsing.


## Deployment

To deploy this rule engine in a production environment (e.g., Databricks, Cloud Functions, Codespace):

1.  **Use the `build` folder**: This folder contains all necessary core files.
    - `rule_engine/`: Core package.
    - `rules/`: JSON rule configurations.
    - `rule_engine_integrated.py`: Main wrapper class.
    - `requirements.txt`: Dependencies.

2.  **Upload/Copy**: Upload the contents of the `build` folder to your target environment.

3.  **Execute**:
    ```python
    import pandas as pd
    import sys
    # Ensure build directory is in path if nested
    sys.path.append("/path/to/build")

    from rule_engine_integrated import PLBRuleEngine

    # Initialize (rules dir is relative to execution or specified)
    engine = PLBRuleEngine(contracts_dir="contracts") # logic defaults to 'rules' dir internally now

    # Process
    df = pd.read_csv("input.csv")
    result_df = engine.process_dataframe(df)
    ```

## Usage

```python
from rule_engine_integrated import PLBRuleEngine

# Initialize
engine = PLBRuleEngine(contracts_dir="contracts")

# Process CSV with JSON output (default)
result = engine.process_csv_file("input.csv", "output/result.json")

# Process CSV with custom output file
result = engine.process_csv("input.csv", "custom/path/results.json")

# Get summary
engine.print_rule_summary()
```

## Features

- **Direct DataFrame Integration**: Seamlessly integrates with Pandas workflows.
- **Row Explosion Logic**: 
    - 1 Input Row -> N Output Rows (where N = number of applicable rules).
    - If no rules apply, 1 row is returned with rule fields empty.
- **Detailed Output**: 
    - Includes Contract IDs, Formulas, Eligibility Reasons, and calculated Tier Payouts (Tier1..10).
    - **IST Timestamps** for processing time.
- **JSON Output**: Comprehensive JSON format with coupon info, eligibility, and contract results
- **UTC/IST Timestamps**: Proper timestamps with timezone information for audit trails
- **Addon Rules**: Dynamic addon rule processing for special eligibility overrides
- **4-Scenario Filter Logic**: Complete IN/OUT filter support for boolean and value-based criteria
- **Tier-based Calculations**: Multiple payout tiers with different percentages
- **Production Ready**: Clean rules environment with comprehensive error handling
- **Performance Optimized**: Cached rules for optimal processing speed

## Output Format

The library generates JSON output with the following structure:
```json
{
  "batch_processing_summary": {
    "processing_timestamp": "2025-09-28T13:58:14.709458+00:00",
    "rule_engine_version": "1.7.0",
    "processing_environment": {
      "python_version": "3.13.5",
      "platform": "nt",
      "hostname": "unknown"
    }
  },
  "coupons": [
    {
      "coupon_info": {...},
      "airline_eligibility": true/false,
      "processing_summary": {
        "processing_timestamp": "2025-09-28T13:58:14.661455+00:00",
        "rule_engine_version": "1.7.0"
      },
      "contract_results": [...]
    }
  ]
}
```

## 🎯 Enhanced Filtering System

### Comprehensive Filtering Criteria

The system now supports 25+ filtering criteria with flexible IN/OUT logic:

#### Supported Filter Types
- **Cabin**: Economy, Business, First class filtering
- **RBD**: Revenue booking designator filtering
- **DomIntl**: Domestic vs International filtering (using `cpn_is_international`)
- **Fare_Type**: Fare type classification
- **OnD**: Origin and Destination filtering
- **Route**: Route-based filtering
- **Flight_Nos**: Flight number filtering
- **Sales_Date**: Sales date range filtering
- **Travel_Date**: Travel date range filtering
- **Corporate_Code**: Corporate account filtering
- **Tour_Code**: Tour code filtering
- **PCCs**: Passenger Control Center filtering
- **City_Codes**: City code filtering
- **POS**: Point of Sale filtering
- **POO**: Point of Origin filtering
- **Code_Share**: Code share flight filtering
- **Interline**: Interline agreement filtering
- **Alliance**: Airline alliance filtering
- **Marketing_Airline**: Marketing airline filtering
- **Operating_Airline**: Operating airline filtering
- **Ticketing_Airline**: Ticketing airline filtering
- **NDC**: New Distribution Capability filtering
- **SITI_SOTO_SITO_SOTI**: Sales location filtering
- **Fare_Basis_Patterns**: Fare basis pattern matching
- **Fare_Class_Logic**: Fare class logic filtering
- **Fare_Basis_Logic**: Fare basis logic filtering

#### IN/OUT Logic Examples

```json
{
  "trigger_eligibility_criteria": {
    "IN": {
      "Code_Share": true,
      "DomIntl": true,
      "Cabin": ["Economy", "Business"]
    },
    "OUT": {
      "Interline": true,
      "Fare_Type": ["Refundable"]
    }
  }
}
```

- **IN Logic**: `{"Code_Share": true}` means if CodeShare is True, then it's eligible
- **OUT Logic**: `{"Code_Share": true}` means if CodeShare is True, then it's NOT eligible (excluded)


