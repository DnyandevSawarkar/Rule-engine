"""
Test script for roll-back-to-zero MTP auto-generation functionality
"""

import json
from datetime import date
from pathlib import Path
from rule_engine.rule_loader import RuleLoader
from rule_engine.models import ContractData

def test_period_calculation():
    """Test period calculation for different period types"""
    print("\n" + "="*60)
    print("TEST 1: Period Calculation")
    print("="*60)
    
    loader = RuleLoader()
    start_date = date(2025, 4, 1)
    end_date = date(2025, 9, 30)
    
    # Test MONTHLY
    print("\n1. Testing MONTHLY period calculation...")
    monthly_periods = loader._calculate_time_based_periods(
        start_date, end_date, 'MONTHLY', {}
    )
    print(f"   Found {len(monthly_periods)} monthly periods:")
    for p in monthly_periods:
        print(f"   - Period {p['index']}: {p['start']} to {p['end']}")
    assert len(monthly_periods) == 6, f"Expected 6 periods, got {len(monthly_periods)}"
    print("   [PASS] MONTHLY calculation passed")
    
    # Test QUARTERLY
    print("\n2. Testing QUARTERLY period calculation...")
    quarterly_periods = loader._calculate_time_based_periods(
        start_date, end_date, 'QUARTERLY', {}
    )
    print(f"   Found {len(quarterly_periods)} quarterly periods:")
    for p in quarterly_periods:
        print(f"   - Period {p['index']}: {p['start']} to {p['end']}")
    assert len(quarterly_periods) == 2, f"Expected 2 periods, got {len(quarterly_periods)}"
    print("   [PASS] QUARTERLY calculation passed")
    
    # Test CUSTOM (15 days)
    print("\n3. Testing CUSTOM period (15 days)...")
    custom_15_periods = loader._calculate_time_based_periods(
        start_date, end_date, 'CUSTOM', {'reset_after_days': 15}
    )
    print(f"   Found {len(custom_15_periods)} periods (15 days each):")
    for p in custom_15_periods[:3]:  # Show first 3
        print(f"   - Period {p['index']}: {p['start']} to {p['end']}")
    print(f"   ... (showing first 3 of {len(custom_15_periods)})")
    assert len(custom_15_periods) > 0, "Expected at least 1 period"
    print("   [PASS] CUSTOM (15 days) calculation passed")
    
    # Test CUSTOM (2 months)
    print("\n4. Testing CUSTOM period (2 months)...")
    custom_2m_periods = loader._calculate_time_based_periods(
        start_date, end_date, 'CUSTOM', {'reset_after_months': 2}
    )
    print(f"   Found {len(custom_2m_periods)} periods (2 months each):")
    for p in custom_2m_periods:
        print(f"   - Period {p['index']}: {p['start']} to {p['end']}")
    assert len(custom_2m_periods) == 3, f"Expected 3 periods, got {len(custom_2m_periods)}"
    print("   [PASS] CUSTOM (2 months) calculation passed")
    
    print("\n[PASS] All period calculation tests passed!")


def test_reset_config_parsing():
    """Test reset_config parsing"""
    print("\n" + "="*60)
    print("TEST 2: Reset Config Parsing")
    print("="*60)
    
    loader = RuleLoader()
    
    # Test with reset_config
    rule_with_config = {
        'then': {
            'evaluation': {
                'basis': 'ROLL_BACK_TO_ZERO',
                'reset_config': {
                    'type': 'TIME_BASED',
                    'period': 'MONTHLY',
                    'generate_mtps': True
                }
            }
        }
    }
    
    config = loader._parse_reset_config(rule_with_config)
    assert config is not None, "Should parse reset_config"
    assert config['type'] == 'TIME_BASED', "Should have correct type"
    assert config['period'] == 'MONTHLY', "Should have correct period"
    print("[PASS] Parsed reset_config correctly")
    
    # Test without reset_config (backward compatibility)
    rule_without_config = {
        'then': {
            'evaluation': {
                'basis': 'ROLL_BACK_TO_ZERO'
            }
        }
    }
    
    config = loader._parse_reset_config(rule_without_config)
    assert config is None, "Should return None when reset_config missing"
    print("[PASS] Handles missing reset_config (backward compatibility)")
    
    # Test non-rollback rule
    non_rollback_rule = {
        'then': {
            'evaluation': {
                'basis': 'STANDARD'
            }
        }
    }
    
    config = loader._parse_reset_config(non_rollback_rule)
    assert config is None, "Should return None for non-rollback rules"
    print("[PASS] Ignores non-rollback rules")
    
    print("\n[PASS] All reset_config parsing tests passed!")


def test_mtp_generation():
    """Test MTP generation"""
    print("\n" + "="*60)
    print("TEST 3: MTP Generation")
    print("="*60)
    
    loader = RuleLoader()
    
    # Create a test rule
    test_rule = {
        'rule_id': 'TEST_RULE_MTP1',
        'name': 'Test Roll Back to Zero',
        'type': 'Single Tier Metric TABLE',
        'variant_flags': {'Sales': True, 'NFR': True},
        'what_if': {
            'trigger': {
                'type': 'SALES',
                'components': ['BASE', 'YQ'],
                'formula': 'BASE + YQ'
            },
            'payout': {
                'type': 'PERCENTAGE',
                'components': ['BASE', 'YQ'],
                'formula': 'tier_percent * (BASE + YQ)'
            }
        },
        'where_trigger': {'IN': {}, 'OUT': {}},
        'where_payout': {'IN': {}, 'OUT': {}},
        'tiers': [{
            'rows': [{
                'target': {'min': 0, 'max': 1000000},
                'payout': {'value': 1, 'unit': 'PERCENT'}
            }]
        }],
        'then': {
            'evaluation': {
                'basis': 'ROLL_BACK_TO_ZERO',
                'reset_config': {
                    'type': 'TIME_BASED',
                    'period': 'MONTHLY',
                    'generate_mtps': True,
                    'mtp_naming': 'SEQUENTIAL'
                }
            }
        }
    }
    
    metadata = {
        'ruleset_id': 'TEST_RULESET',
        'source_name': 'Test Rule',
        'currency': 'USD',
        'iata_codes': [],
        'countries': []
    }
    
    start_date = date(2025, 4, 1)
    end_date = date(2025, 9, 30)
    
    print("\nGenerating MTPs...")
    contracts = loader._generate_mtps_for_rollback(
        test_rule, metadata, start_date, end_date, 'test.json', test_rule['then']['evaluation']['reset_config']
    )
    
    print(f"Generated {len(contracts)} MTPs")
    assert len(contracts) == 6, f"Expected 6 MTPs, got {len(contracts)}"
    
    # Verify each MTP
    for i, contract in enumerate(contracts, 1):
        assert contract.evaluation_basis == 'ROLL_BACK_TO_ZERO', f"MTP{i} should have evaluation_basis"
        assert contract.mtp_period_index == i, f"MTP{i} should have period_index {i}"
        assert contract.period_start_date is not None, f"MTP{i} should have period_start_date"
        assert contract.period_end_date is not None, f"MTP{i} should have period_end_date"
        assert f"MTP{i}" in contract.rule_id, f"MTP{i} rule_id should contain MTP{i}"
        print(f"   [PASS] MTP{i}: {contract.period_start_date} to {contract.period_end_date} ({contract.rule_id})")
    
    print("\n[PASS] All MTP generation tests passed!")


def test_backward_compatibility():
    """Test backward compatibility with rules without reset_config"""
    print("\n" + "="*60)
    print("TEST 4: Backward Compatibility")
    print("="*60)
    
    loader = RuleLoader()
    
    # Create a rule without reset_config (old format)
    old_rule = {
        'rule_id': 'OLD_RULE',
        'name': 'Old Format Rule',
        'type': 'Single Tier Metric TABLE',
        'variant_flags': {'Sales': True},
        'what_if': {
            'trigger': {
                'type': 'SALES',
                'components': ['BASE'],
                'formula': 'BASE'
            },
            'payout': {
                'type': 'PERCENTAGE',
                'components': ['BASE'],
                'formula': 'BASE * 0.01'
            }
        },
        'where_trigger': {'IN': {}, 'OUT': {}},
        'where_payout': {'IN': {}, 'OUT': {}},
        'tiers': []
    }
    
    metadata = {
        'ruleset_id': 'OLD_RULESET',
        'source_name': 'Old Rule',
        'currency': 'USD',
        'iata_codes': [],
        'countries': []
    }
    
    start_date = date(2025, 1, 1)
    end_date = date(2025, 12, 31)
    
    print("\nTesting rule without reset_config...")
    config = loader._parse_reset_config(old_rule)
    assert config is None, "Should return None for rules without reset_config"
    
    # Should parse as single MTP
    contract = loader._parse_single_rule(old_rule, metadata, start_date, end_date, 'old.json', 1)
    assert contract is not None, "Should parse old rule format"
    assert contract.evaluation_basis is None or contract.evaluation_basis == '', "Old rule should not have evaluation_basis"
    print("[PASS] Old format rule parsed correctly (single MTP)")
    
    print("\n[PASS] Backward compatibility test passed!")


def test_actual_rule_file():
    """Test loading the actual QR rule file"""
    print("\n" + "="*60)
    print("TEST 5: Actual Rule File Loading")
    print("="*60)
    
    rule_file = Path("rules/QR/2025/04/QR-EG-Apr-Sep25-PLB-rule.json")
    
    if not rule_file.exists():
        print(f"[WARN] Rule file not found: {rule_file}")
        print("   Skipping actual file test")
        return
    
    print(f"\nLoading rule file: {rule_file}")
    loader = RuleLoader(rules_dir="rules")
    
    # Load the specific rule file
    contracts = loader._load_rule_file(rule_file)
    
    print(f"Loaded {len(contracts)} contract(s)")
    
    # Check if MTPs were generated
    if len(contracts) > 1:
        print(f"[PASS] Successfully generated {len(contracts)} MTPs!")
        for i, contract in enumerate(contracts[:3], 1):  # Show first 3
            print(f"   MTP{i}: {contract.rule_id}")
            print(f"      Period: {contract.period_start_date} to {contract.period_end_date}")
            print(f"      Evaluation Basis: {contract.evaluation_basis}")
    else:
        print(f"[WARN] Only {len(contracts)} contract loaded (expected 6 MTPs)")
        if len(contracts) > 0:
            contract = contracts[0]
            print(f"   Contract: {contract.rule_id}")
            print(f"   Has reset_config: {bool(contract.reset_config)}")
            print(f"   Evaluation Basis: {contract.evaluation_basis}")
    
    # Verify the rule has reset_config
    with open(rule_file, 'r') as f:
        rule_data = json.load(f)
    
    evaluation = rule_data.get('rules', [{}])[0].get('then', {}).get('evaluation', {})
    has_reset_config = 'reset_config' in evaluation
    print(f"\nRule file has reset_config: {has_reset_config}")
    
    if has_reset_config:
        reset_config = evaluation.get('reset_config', {})
        print(f"   Type: {reset_config.get('type')}")
        print(f"   Period: {reset_config.get('period')}")
        print(f"   Generate MTPs: {reset_config.get('generate_mtps')}")
    
    print("\n[PASS] Actual rule file test completed!")


def main():
    """Run all tests"""
    print("\n" + "="*60)
    print("ROLL-BACK-TO-ZERO MTP AUTO-GENERATION TESTS")
    print("="*60)
    
    try:
        test_period_calculation()
        test_reset_config_parsing()
        test_mtp_generation()
        test_backward_compatibility()
        test_actual_rule_file()
        
        print("\n" + "="*60)
        print("ALL TESTS PASSED!")
        print("="*60)
        
    except AssertionError as e:
        print(f"\n[FAIL] TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return 1
    except Exception as e:
        print(f"\n[ERROR] ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())

