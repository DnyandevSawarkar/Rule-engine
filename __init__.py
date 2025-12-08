"""
Rule Engine Library - Simple wrapper around the main codebase
"""

# Import everything from the main integrated file
from .rule_engine_integrated import PLBRuleEngine

# Simple wrapper class
class RuleEngine:
    """
    Simple wrapper around PLBRuleEngine
    """
    
    def __init__(self, rules_dir: str = "rules"):
        """
        Initialize the Rule Engine
        
        Args:
            rules_dir: Directory containing rule JSON files
        """
        self.rules_dir = rules_dir
        self.engine = PLBRuleEngine()
    
    def process_csv(self, input_csv_path: str, output_file: str = None, output_format: str = "json"):
        """
        Process CSV file with rules and generate JSON output
        
        Args:
            input_csv_path: Path to input CSV file
            output_file: Path to output JSON file (defaults to output/result.json)
            output_format: Output format - "json" or "csv" (defaults to json)
        """
        return self.engine.process_csv_file(input_csv_path, output_file, output_format)
    
    def get_contract_summary(self):
        """Get rule summary (contracts are loaded from rules directory)"""
        rules = self.engine.get_available_rules()
        
        return {
            'total_rules': len(rules),
            'rules': rules
        }
    
    def print_rule_summary(self):
        """Print rule summary"""
        summary = self.get_contract_summary()
        print("\n" + "="*60)
        print("RULE ENGINE SUMMARY")
        print("="*60)
        print(f"Total Rules: {summary.get('total_rules', 0)}")
        
        if 'rules' in summary and summary['rules']:
            print("\nAvailable Rules:")
            for rule in summary['rules']:
                print(f"  - {rule.get('source_name', 'Unknown')}: {rule.get('rule_count', 0)} rules")
                print(f"    File: {rule.get('file_name', 'Unknown')}")
                print(f"    Airline: {rule.get('airline', 'Unknown')}")
                print(f"    Period: {rule.get('year', 'Unknown')}-{rule.get('month', 'Unknown')}")
        print("="*60)

__version__ = "1.7.0"
__author__ = "Dnyandev Sawarkar"
__author_email__ = "dnyandev@prodt.co"
__github__ = "https://github.com/DnyandevSawarkar"
__all__ = ["RuleEngine", "PLBRuleEngine"]
