# Dynamodb Data Loader

## Usage

RTFM!

```bash
# From data-loader directory:
python3 main.py -h

# Dry run by default
time AWS_PROFILE=myprofile python3 main.py -f my-template.json -t my-table-name -n 3

# Commit true to write to ddb
time AWS_PROFILE=myprofile python3 main.py -f my-template.json -t my-table-name -n 3 --commit true

```
