# lspd-export

Export your Lightspeed X-Series POS data to CSV files for database import.

## Installation

```bash
# Clone the repository
git clone https://github.com/workingman/lspd-export.git
cd lspd-export

# Install dependencies
pip install -r requirements.txt
```

## Configuration

1. Copy `.env.example` to `.env`:
   ```bash
   cp .env.example .env
   ```

2. Edit `.env` with your Lightspeed credentials:
   - `LIGHTSPEED_DOMAIN`: Your store domain (e.g., `store.retail.lightspeed.app`)
   - `LIGHTSPEED_TOKEN`: Your personal access token from Lightspeed

**⚠️ IMPORTANT**: Never commit your `.env` file to version control!

## Usage

Run the export script:

```bash
python3 export_lightspeed_data.py
```

### Features

- **Interruptible**: Press `Ctrl+C` to safely stop the export at any time
- **Resumable**: Restart the script to continue from where you left off
- **Progress tracking**: Real-time progress bars and checkpoint saving
- **Error handling**: Continues export even if individual endpoints fail

### Export Process

The script will:
1. Check for incomplete exports and offer to resume them
2. Connect to your Lightspeed account via API
3. Download all available POS data with progress tracking
4. Export data to CSV files in `./exports/[timestamp]/`
5. Save checkpoints after each completed endpoint
6. Create detailed log files for troubleshooting

### Resuming Interrupted Exports

If an export is interrupted:
- Your progress is automatically saved
- Run the script again to see resume options
- Choose to continue an existing export or start fresh
- The script will skip completed endpoints and resume from where it stopped

## Exported Data

The following CSV files will be created:
- **Sales**: `sales.csv`, `sale_items.csv`, `sale_payments.csv`
- **Products**: `products.csv`, `product_variants.csv`, `inventory.csv`
- **Customers**: `customers.csv`, `customer_groups.csv`
- **Operations**: `outlets.csv`, `registers.csv`, `users.csv`
- **Financial**: `taxes.csv`, `payment_types.csv`
- **Suppliers**: `suppliers.csv`, `brands.csv`, `product_types.csv`
- And more...

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License

[MIT](https://choosealicense.com/licenses/mit/)