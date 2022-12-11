-- PLEASE NOTE
-- Buffer tables are what I personally use to batch inserts
-- You may have to modify them to work with your setup

CREATE TABLE apc_ups (
    name LowCardinality(String),
    model LowCardinality(Nullable(String)),
    sku LowCardinality(Nullable(String)),
    manufacture_date Nullable(Date),
    sensitivity LowCardinality(Nullable(String)),
    status LowCardinality(Nullable(String)),
    last_transfer_reason LowCardinality(Nullable(String)),
    battery_last_replace_date Nullable(Date),
    battery_next_replace_date Nullable(Date),
    battery_needs_replacement Nullable(bool),
    battery_status LowCardinality(Nullable(String)),
    battery_capacity_percent float,
    battery_voltage float,
    runtime_remaining_seconds bigint,
    on_battery_seconds bigint,
    input_voltage float,
    input_frequency Nullable(float),
    output_voltage Nullable(float),
    output_frequency Nullable(float),
    output_load_percent float,
    output_load_watts Nullable(smallint),
    output_load_va Nullable(smallint),
    output_current_amps Nullable(float),
    output_efficiency_percent Nullable(float),
    output_energy_usage_kwh Nullable(float),
    sensor_name Array(LowCardinality(String)), -- Array of sensor names (i.e. "Port 1 Temp 1 Temperature")
    sensor_value Array(float), -- Array of sensor values (i.e. "19.5")
    time DateTime DEFAULT now()
) ENGINE = MergeTree() PARTITION BY toYYYYMM(time) ORDER BY (name, time) PRIMARY KEY (name, time);

CREATE TABLE apc_ups_buffer (
    name LowCardinality(String),
    model LowCardinality(Nullable(String)),
    sku LowCardinality(Nullable(String)),
    manufacture_date Nullable(Date),
    sensitivity LowCardinality(Nullable(String)),
    status LowCardinality(Nullable(String)),
    last_transfer_reason LowCardinality(Nullable(String)),
    battery_last_replace_date Nullable(Date),
    battery_next_replace_date Nullable(Date),
    battery_needs_replacement Nullable(bool),
    battery_status LowCardinality(Nullable(String)),
    battery_capacity_percent float,
    battery_voltage float,
    runtime_remaining_seconds bigint,
    on_battery_seconds bigint,
    input_voltage float,
    input_frequency Nullable(float),
    output_voltage Nullable(float),
    output_frequency Nullable(float),
    output_load_percent float,
    output_load_watts Nullable(smallint),
    output_load_va Nullable(smallint),
    output_current_amps Nullable(float),
    output_efficiency_percent Nullable(float),
    output_energy_usage_kwh Nullable(float),
    sensor_name Array(LowCardinality(String)), -- Array of sensor names (i.e. "Port 1 Temp 1 Temperature")
    sensor_value Array(float), -- Array of sensor values (i.e. "19.5")
    time DateTime DEFAULT now()
) ENGINE = Buffer(homelab, apc_ups, 1, 10, 10, 10, 100, 10000, 10000);