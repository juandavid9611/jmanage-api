# Migration Scripts

This directory contains database migration scripts for the jmanage-api application.

## add_account_id_migration.py

Migration script to add `account_id` to all existing DynamoDB records for multi-tenancy support.

### Prerequisites

1. **Environment Variables**: Ensure all table name environment variables are set in your `.env` file:
   - `USER_TABLE_NAME`
   - `TOUR_TABLE_NAME`
   - `WORKSPACE_TABLE_NAME`
   - `PRODUCT_TABLE_NAME`
   - `ORDER_TABLE_NAME`
   - `PAYMENT_REQUEST_TABLE_NAME`
   - `CALENDAR_TABLE_NAME`

2. **AWS Credentials**: Ensure AWS credentials are configured with permissions to:
   - Scan DynamoDB tables
   - Update DynamoDB items

3. **Python Dependencies**: Install required packages:
   ```bash
   pip install boto3 python-dotenv
   ```

### Usage

#### Dry Run (Recommended First Step)

Test the migration without making any changes:

```bash
python migrations/add_account_id_migration.py --account-id acc_default --dry-run
```

This will:
- Scan all tables
- Log what would be updated
- Not make any actual changes

#### Migrate All Tables

```bash
python migrations/add_account_id_migration.py --account-id acc_default
```

You will be prompted to confirm before proceeding.

#### Migrate Specific Table

```bash
python migrations/add_account_id_migration.py --account-id acc_default --table users
```

Available tables:
- `users`
- `tours`
- `workspaces`
- `products`
- `orders`
- `payment_requests`
- `calendar`

#### Debug Mode

For more detailed logging:

```bash
python migrations/add_account_id_migration.py --account-id acc_default --log-level DEBUG
```

### What It Does

1. **Scans** each DynamoDB table
2. **Identifies** records without `account_id`
3. **Updates** records with the specified default `account_id`
4. **Skips** records that already have `account_id`
5. **Logs** all operations to console and file

### Output

- **Console**: Real-time progress and summary
- **Log File**: Detailed log saved as `migration_YYYYMMDD_HHMMSS.log`

### Safety Features

- **Dry Run Mode**: Test before making changes
- **Conditional Updates**: Only updates items without `account_id`
- **Error Handling**: Continues on errors, logs all issues
- **Confirmation Prompt**: Requires explicit confirmation for production runs
- **Detailed Logging**: All operations logged for audit trail

### Example Output

```
================================================================================
Migrating table: users
================================================================================
Scanning table jmanage-users-prod...
Scanned 150 items from jmanage-users-prod
Found 150 items in jmanage-users-prod
150 items need account_id added
Progress: 100/150 items processed
Successfully updated 150 items in jmanage-users-prod

================================================================================
MIGRATION SUMMARY
================================================================================
Total records scanned: 1050
Total records updated: 1050
Total records skipped: 0
Total errors: 0
================================================================================
```

### Rollback

If you need to rollback:

1. **Remove account_id**: Create a reverse migration script
2. **Restore from backup**: Use DynamoDB point-in-time recovery
3. **Manual cleanup**: Use AWS Console or CLI to remove the attribute

### Post-Migration Steps

After successful migration:

1. **Verify Data**: Check a sample of records to ensure `account_id` is set correctly
2. **Update Infrastructure**: Deploy DynamoDB GSI for `account_id`
3. **Deploy Code**: Deploy updated application code that uses `account_id`
4. **Monitor**: Watch for errors in application logs

### Troubleshooting

**Error: "Table name not found in environment"**
- Ensure all `*_TABLE_NAME` environment variables are set in `.env`

**Error: "ConditionalCheckFailedException"**
- This is expected for items that already have `account_id` (they're skipped)

**Error: "AccessDeniedException"**
- Ensure AWS credentials have DynamoDB permissions

**Migration is slow**
- This is normal for large tables
- Consider migrating tables individually
- Monitor DynamoDB consumed capacity
