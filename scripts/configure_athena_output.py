"""
Configure Athena Workgroup Settings
Sets the output location for query results
"""

import boto3

def configure_athena_workgroup():
    """Configure Athena workgroup with output location"""
    athena = boto3.client('athena', region_name='us-east-1')
    
    workgroup_name = 'primary'  # Default workgroup
    output_location = 's3://modern-data-stack-athena-results/'
    
    print("=" * 70)
    print("  CONFIGURING ATHENA WORKGROUP")
    print("=" * 70)
    print()
    
    try:
        # Update workgroup configuration
        athena.update_work_group(
            WorkGroup=workgroup_name,
            ConfigurationUpdates={
                'ResultConfigurationUpdates': {
                    'OutputLocation': output_location
                },
                'EnforceWorkGroupConfiguration': True
            }
        )
        
        print(f"✓ Workgroup '{workgroup_name}' configured successfully")
        print(f"  Output location: {output_location}")
        print()
        print("You can now run queries in Athena!")
        print()
        return True
    
    except Exception as e:
        print(f"✗ Error: {e}")
        print()
        print("Alternative: Configure manually in Athena UI")
        print("  1. Go to Athena → Settings")
        print(f"  2. Set Query result location to: {output_location}")
        print("  3. Click Save")
        print()
        return False


if __name__ == "__main__":
    configure_athena_workgroup()
