# apc-ups-clickhouse #
An APC UPS exporter for ClickHouse

Configuration is done via environment variables and targets.json

## Environment Variables ##
```
=== Exporter ===
DATA_QUEUE_LIMIT    -   ClickHouse insert queue max size (default: "50")
FETCH_INTERVAL      -   Default fetch interval in seconds (default: "30")
FETCH_TIMEOUT       -   Default fetch timeout in seconds (default: "15")
LOG_LEVEL           -   Logging verbosity (default: "20"), levels: 0 (debug) / 10 (info) / 20 (warning) / 30 (error) / 40 (critical)

=== ClickHouse ===
CLICKHOUSE_URL      -   ClickHouse URL (i.e. "http://192.168.0.69:8123")
CLICKHOUSE_USER     -   ClickHouse login username
CLICKHOUSE_PASS     -   ClickHouse login password
CLICKHOUSE_DB       -   ClickHouse database
CLICKHOUSE_TABLE    -   ClickHouse table to insert to (default: "apc_ups")
```

## targets.json ##

Used to configure UPS targets. Example config in `targets.example.json`

```
[
    {
        "name": "UPS name",
        "ip": "UPS NMC IP",
        "sku": "UPS SKU (optional, for older models that don't provide it via SNMP)",
        "rated_va": "UPS VA rating (optional, for older models that don't provide it via SNMP)",
        "rated_watts": "UPS watt rating (optional, for older models that don't provide it via SNMP)",
        "snmp_version": "SNMP version (v2c or v3)",
        "snmp_community": "SNMP community (v2c only)",
        "snmp_username": "SNMP username (v3 only)",
        "snmp_password": "SNMP password (v3 only)",
        "snmp_port": "SNMP port (optional, default: 161),
        "interval": "Fetch interval (overrides the default interval)",
        "timeout": "Fetch timeout (overrides the default timeout)",
        "fetch_probes": "Method to use for fetching probe data (off, snmp, http, https)",
        "http_username": "Web UI username for fetching HTML probe data",
        "http_password": "Web UI password for fetching HTML probe data",
        "http_port": "Web UI port for fetching HTML probe data"
    }
]
```