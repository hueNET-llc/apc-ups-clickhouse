import aiochclient
import aiohttp
import asyncio
import colorlog
import datetime
import ujson as json
import logging
import os
import re
import signal
import sys
import uvloop
uvloop.install()

log = logging.getLogger('APC')


class APC:
    def __init__(self, loop):
        # Setup logging
        self._setup_logging()
        # Load environment variables
        self._load_env_vars()

        # NMC session regex
        self.nmc_session_regex = re.compile(
            r'/NMC/(.*)/'
        )

        # Probe HTML regex
        self.probe_html_regex = re.compile(
            r'<a href=\"uiocfg\.htm\?sensor=[\d]{1}\" alt=\"Edit\" title=\"Edit\">([^<]*)</a></td>\r\n<td><span class=\"se-icon-f4-selection text-success\"></span>&nbsp;Normal</td>\r\n<td>([^&]*)&deg;&nbsp;(F|C)</td>\r\n<td>(?:([\d]{1,2})%&nbsp;RH|Not Available)</td>\r\n</tr>\r\n(?=<tr>|</table>\n</div>\n</div>\n<div class=\"dataSection\">\n<div class=\"dataSubHeader\">\n<span id=\"langInputContacts\">)'
        )

        # Get the event loop
        self.loop = loop

        self.ups_targets = []

        # Queue of data waiting to be inserted into ClickHouse
        self.clickhouse_queue = asyncio.Queue(maxsize=self.clickhouse_queue_limit)

        # Event used to stop the loop
        self.stop_event = asyncio.Event()

    def _setup_logging(self):
        """
            Sets up logging colors and formatting
        """
        # Create a new handler with colors and formatting
        shandler = logging.StreamHandler(stream=sys.stdout)
        shandler.setFormatter(colorlog.LevelFormatter(
            fmt={
                'DEBUG': '{log_color}{asctime} [{levelname}] {message}',
                'INFO': '{log_color}{asctime} [{levelname}] {message}',
                'WARNING': '{log_color}{asctime} [{levelname}] {message}',
                'ERROR': '{log_color}{asctime} [{levelname}] {message}',
                'CRITICAL': '{log_color}{asctime} [{levelname}] {message}',
            },
            log_colors={
                'DEBUG': 'blue',
                'INFO': 'white',
                'WARNING': 'yellow',
                'ERROR': 'red',
                'CRITICAL': 'bg_red',
            },
            style='{',
            datefmt='%H:%M:%S'
        ))
        # Add the new handler
        logging.getLogger('APC').addHandler(shandler)
        log.debug('Finished setting up logging')

    def _load_targets(self):
        # Open and read the targets file
        with open('targets.json', 'r') as file:
            try:
                targets = json.loads(file.read())
            except Exception as e:
                log.error(f'Failed to read targets.json: "{e}"')
                exit(1)

        # Parse targets
        for target in targets:
            try:
                if (snmp_version := target['snmp_version'].lower()) not in ('v2c', 'v3'):
                    log.error(f'Invalid snmp_version "{target["snmp_version"]}" for target "{target["name"]}"')
                    continue
                
                if target['snmp_version'] == 'v2c' and not target.get('snmp_community'):
                    log.error(f'Missing snmp_community for target "{target["name"]}"')
                    continue
                
                if target['snmp_version'] == 'v3' and (not target.get('snmp_username') or not target.get('snmp_password')):
                    log.error(f'Missing snmp_username/snmp_password for target "{target["name"]}"')
                    continue

                if (snmp_port := target.get('port', 161)):
                    try:
                        snmp_port = int(snmp_port)
                    except ValueError:
                        log.error(f'Invalid snmp_port "{snmp_port}" for target "{target["name"]}"')
                        continue

                if (interval := target.get('interval', self.fetch_interval)):
                    try:
                        interval = int(interval)
                    except ValueError:
                        log.error(f'Invalid interval "{interval}" for target "{target["name"]}"')
                        continue

                if (timeout := target.get('timeout', self.fetch_timeout)):
                    try:
                        timeout = int(timeout)
                    except ValueError:
                        log.error(f'Invalid timeout "{timeout}" for target "{target["name"]}"')
                        continue
                
                if (fetch_probes := target.get('fetch_probes', 'off').lower()):
                    if fetch_probes == 'off':
                        fetch_probes = False
                    elif fetch_probes not in ('snmp', 'http', 'https'):
                        log.error(f'Invalid fetch_probes value "{fetch_probes}" for target "{target["name"]}"')
                        continue

                if fetch_probes in ('http', 'https'):
                    if not target.get('http_username') or not target.get('http_password'):
                        log.error(f'Missing http_username/http_password for target "{target["name"]}"')
                        continue

                if (http_port := target.get('http_port')):
                    try:
                        http_port = int(http_port)
                    except ValueError:
                        log.error(f'Invalid http_port "{http_port}" for target "{target["name"]}"')
                        continue
                elif fetch_probes == 'http':
                    http_port = 80
                elif fetch_probes == 'https':
                    http_port = 443
                
                if (rated_va := target.get('rated_va')):
                    try:
                        rated_va = int(rated_va)
                    except ValueError:
                        log.error(f'Invalid rated_va value "{rated_va}" for target "{target["name"]}"')
                        continue
                
                if (rated_watts := target.get('rated_watts')):
                    try:
                        rated_watts = int(rated_watts)
                    except ValueError:
                        log.error(f'Invalid rated_watts value "{rated_watts}" for target "{target["name"]}"')
                        continue

                self.ups_targets.append({
                    'name': target['name'], # UPS name
                    'ip': target['ip'], # UPS IP
                    'sku': target.get('sku'), # UPS SKU
                    'rated_va': rated_va, # UPS rated VA
                    'rated_watts': rated_watts, # UPS rated watts
                    'snmp_version': snmp_version, # SNMP version
                    'snmp_community': target.get('snmp_community'), # SNMP community (v2c)
                    'snmp_username': target.get('snmp_username'), # SNMP username (v3)
                    'snmp_password': target.get('snmp_password'), # SNMP password (v3)
                    'snmp_port': snmp_port, # SNMP port
                    'interval': interval, # Fetch interval
                    'timeout': timeout, # SNMP fetch timeout
                    'fetch_probes': fetch_probes, # off, snmp, http, or https (scraping the web interface for more precise data)
                    'http_username': target.get('http_username'), # HTTP username
                    'http_password': target.get('http_password'), # HTTP password
                    'http_port': http_port, # HTTP port
                    'nmc_session': None # Used internally for saving NMC session
                })
                log.debug(f'Parsed UPS target "{target["name"]}" at IP "{target["ip"]}"')
            except KeyError as e:
                log.error(f'Missing required key "{e.args[0]}" for UPS target "{target["name"]}"')
            except Exception:
                log.exception(f'Failed to parse UPS target {target}')

    def _load_env_vars(self):
        """
            Loads environment variables
        """
        # Max number of inserts waiting to be inserted at once
        try:
            self.clickhouse_queue_limit = int(os.environ.get('CLICKHOUSE_QUEUE_LIMIT', 50))
        except ValueError:
            log.exception('Invalid CLICKHOUSE_QUEUE_LIMIT passed, must be a number')
            exit(1)

        # Default global SNMP fetch interval
        try:
            self.fetch_interval = int(os.environ.get('FETCH_INTERVAL', 30))
        except ValueError:
            log.exception('Invalid FETCH_INTERVAL passed, must be a number')
            exit(1)

        # Default global SNMP fetch timeout
        try:
            self.fetch_timeout = int(os.environ.get('FETCH_TIMEOUT', 15))
        except ValueError:
            log.exception('Invalid FETCH_TIMEOUT passed, must be a number')
            exit(1)

        # Log level to use
        # 10/debug  20/info  30/warning  40/error
        try:
            self.log_level = int(os.environ.get('LOG_LEVEL', 20))
        except ValueError:
            log.exception('Invalid LOG_LEVEL passed, must be a number')
            exit(1)

        # Set the logging level
        logging.root.setLevel(self.log_level)

        # ClickHouse info
        try:
            self.clickhouse_url = os.environ['CLICKHOUSE_URL']
            self.clickhouse_user = os.environ['CLICKHOUSE_USER']
            self.clickhouse_pass = os.environ['CLICKHOUSE_PASS']
            self.clickhouse_db = os.environ['CLICKHOUSE_DB']
        except KeyError as e:
            log.error(f'Missing required environment variable "{e.args[0]}"')
            exit(1)
        self.clickhouse_table = os.environ.get('CLICKHOUSE_TABLE', 'apc_ups')

    async def insert_to_clickhouse(self):
        """
            Gets data from the data queue and inserts it into ClickHouse
        """
        while True:
            # Get and check data from the queue
            if not (data := await self.clickhouse_queue.get()):
                continue

            # Keep trying until the insert succeeds
            while True:
                try:
                    # Insert the data into ClickHouse
                    log.debug(f'Got data to insert: {data}')
                    await self.clickhouse.execute(
                        f"""
                        INSERT INTO {self.clickhouse_table} (
                            name, model, sku, sensitivity, status, last_transfer_reason, battery_needs_replacement,
                            battery_status, output_load_watts, output_load_va, battery_capacity_percent, battery_voltage,
                            input_voltage, input_frequency, output_voltage, output_frequency,
                            output_load_percent, output_current_amps, output_efficiency_percent, output_energy_usage_kwh,
                            manufacture_date, battery_last_replace_date, battery_next_replace_date, runtime_remaining_seconds,
                            on_battery_seconds, sensor_name, sensor_value, time
                        ) VALUES
                        """,
                        data
                    )
                    log.debug(f'Inserted data for timestamp {data[-1]}')
                    # Insert succeeded, break the loop and move on
                    break
                except Exception as e:
                    # Insertion failed
                    log.error(f'Insert failed for timestamp {data[-1]}: "{e}"')
                    # Wait before retrying so we don't spam retries
                    await asyncio.sleep(2)

    async def fetch_snmp(self, ip, version, oids, community:str='', username:str='', password:str='', port:int=161, timeout:int=15) -> dict:
        args = ['snmpbulkget']
        if version == 'v2c':
            args.extend([
                '-v2c',
                '-c', community
            ])
        else:
            args.extend([
                '-v3',
                '-u', username,
                '-A', password,
            ])
        args.extend([
            '-t', f'{timeout or self.fetch_timeout}', # Get timeout
            '-r', '0', # No retries
            '-m', './powernet.mib', # Use the Powernet MIB
            '-Oqs', # Output format: OID, type, value,
            ip # UPS NMC IP
        ])
        snmp_data = {}

        # Iterate through and parse the OIDs
        for oid in oids:
            # Run the snmpbulkget command
            proc = await asyncio.create_subprocess_exec(
                *args + oid.split(),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            # Wait for the command to finish
            stdout, stderr = await proc.communicate()
            # Check for errors
            if proc.returncode != 0:
                log.error(f'snmpbulkget exited with code {proc.returncode} for UPS {ip}')
                return {}

            # SNMP data
            for line in stdout.decode().splitlines():
                line = line.split(' ')
                # Get the OID
                oid = line[0]
                # Get the value and join it back together if it was split
                value = ' '.join(line[1:])
                snmp_data[oid] = value.lstrip('"').rstrip('"')

        return snmp_data
    
    async def generate_nmc_session(self, ups):
        log.info(f'Generating NMC session for UPS "{ups["name"]}" at IP "{ups["ip"]}"')
        try:
            async with self.session.post(
                f'{ups["fetch_probes"]}://{ups["ip"]}:{ups["http_port"]}/Forms/login1',
                data={
                    'prefLanguage': '00000000',
                    'login_username': ups['http_username'],
                    'login_password': ups['http_password'],
                    'submit': 'Log On'
                },
                timeout=ups['timeout']
            ) as resp:
                if resp.status != 200:
                    log.error(f'Failed to generate NMC session for UPS "{ups["name"]}" at IP "{ups["ip"]}": Got HTTP {resp.status} {resp.reason}')
                    return
                # Get the session from the URL
                session = self.nmc_session_regex.search(f'{resp.url}')
                if session:
                    session = session.group(1)
                    log.info(f'Generated NMC session "{session}" for UPS "{ups["name"]}" at IP "{ups["ip"]}"')
                # No session in the URL, username or password is wrong
                else:
                    log.error(f'Failed to generate NMC session for UPS "{ups["name"]}" at IP "{ups["ip"]}": Invalid username or password')
                return session
        except Exception as e:
            log.error(f'Failed to generate NMC session for UPS "{ups["name"]}" at IP "{ups["ip"]}": "{e}"')
            return

    async def fetch_ups(self, ups):
        log.info(f'Starting fetch for UPS "{ups["name"]}" at IP "{ups["ip"]}"')

        # List of OIDs to GETBULK
        # Have to batch them since it exceeds the max packet size
        oids = tuple([
            '.1.3.6.1.4.1.318.1.1.1.1.2 .1.3.6.1.4.1.318.1.1.1.2.2 .1.3.6.1.4.1.318.1.1.1.2.3 .1.3.6.1.4.1.318.1.1.1.4.2 .1.3.6.1.4.1.318.1.1.1.1.1.1',
            '.1.3.6.1.4.1.318.1.1.1.3.3 .1.3.6.1.4.1.318.1.1.1.4.3 .1.3.6.1.4.1.318.1.1.1.12.1 .1.3.6.1.4.1.318.1.1.1.5.2'
        ])
        probe_oid = tuple(['.1.3.6.1.4.1.318.1.1.25.1'])
        while True:
            try:
                # Fetch the SNMP data
                snmp_data = await self.fetch_snmp(
                    ip=ups['ip'],
                    version=ups['snmp_version'],
                    oids=oids,
                    community=ups['snmp_community'],
                    username=ups['snmp_username'],
                    password=ups['snmp_password'],
                    port=ups['snmp_port'],
                    timeout=ups['timeout']
                )
                # Check if there was no data (failed to fetch)
                if not snmp_data:
                    log.error(f'Failed to fetch SNMP data from UPS "{ups["name"]}" at IP "{ups["ip"]}"')
                    # Wait before retrying
                    await asyncio.sleep(ups['interval'] or self.fetch_interval)
                    continue

                # Get the current UTC timestamp
                timestamp = datetime.datetime.now(tz=datetime.timezone.utc).timestamp()
                log.debug(f'Got snmp_data {snmp_data}')

                data = [
                    ups['name'], # Target/UPS name
                    snmp_data.get('upsBasicIdentModel.0'), # upsBasicIdentModel.0 = "Smart-UPS X 2200"
                    snmp_data.get('upsAdvIdentSkuNumber.0', ups['sku']), # upsAdvIdentSkuNumber.0 = "SMX2200RMLV2U"
                    snmp_data.get('upsAdvConfigSensitivity.0'), # Sensitivity : auto(1), low(2), medium(3), high(4)
                    snmp_data.get('upsBasicOutputStatus.0'), # Status : unknown(1), onLine(2), onBattery(3), onSmartBoost(4), timedSleeping(5), softwareBypass(6), off(7), rebooting(8), switchedBypass(9), hardwareFailureBypass(10), sleepingUntilPowerReturn(11), onSmartTrim(12)
                    snmp_data.get('upsAdvInputLineFailCause.0'), # Last transfer reason : noTransfer(1), highLineVoltage(2), brownout(3), blackout(4), smallMomentarySag(5), deepMomentarySag(6), smallMomentarySpike(7), largeMomentarySpike(8), selfTest(9), rateOfVoltageChange(10)
                    snmp_data.get('upsAdvBatteryReplaceIndicator.0') == 'batteryNeedsReplacing', # Battery replace indicator : noBatteryNeedsReplacing(1), batteryNeedsReplacing(2)
                    snmp_data.get('upsBasicBatteryStatus.0'), # Battery status : unknown(1), batteryNormal(2), batteryLow(3)
                ]

                # Output watts
                # Check if the UPS already has a value for this
                if snmp_data.get('upsAdvOutputActivePower.0'):
                    data.append(snmp_data.get('upsAdvOutputActivePower.0'))
                else:
                    # Older models don't have this, so try to calculate it ourselves
                    # Check if the user supplied the UPS rated watts
                    if ups['rated_watts'] and snmp_data.get('upsHighPrecOutputLoad.0') is not None:
                        # Calculate the output watts from the output percent and rated watts
                        data.append(ups['rated_watts'] * (int(snmp_data['upsHighPrecOutputLoad.0']) / 1000))
                    else:
                        # No rated watts, can't calculate output watts
                        data.append(None)
                
                # Output VA
                # Check if the UPS already has a value for this
                if snmp_data.get('upsAdvOutputApparentPower.0'):
                    data.append(snmp_data.get('upsAdvOutputApparentPower.0'))
                else:
                    # Older models don't have this, so try to calculate it ourselves
                    # Check if the user supplied the UPS rated VA
                    if ups['rated_va'] and snmp_data.get('upsHighPrecOutputLoad.0') is not None:
                        # Calculate the output VA from the output percent and rated VA
                        data.append(ups['rated_va'] * (int(snmp_data['upsHighPrecOutputLoad.0']) / 1000))
                    else:
                        # No rated VA, can't calculate output VA
                        data.append(None)
                
                # High precision data parsing
                # Battery capacity : upsHighPrecBatteryCapacity.0 = 1000
                if snmp_data.get('upsHighPrecBatteryCapacity.0'):
                    data.append(float(snmp_data.get('upsHighPrecBatteryCapacity.0', 0.0)) / 10)
                else:
                    data.append(None)
                
                # Battery voltage : upsHighPrecBatteryActualVoltage.0 = 1330
                if snmp_data.get('upsHighPrecBatteryActualVoltage.0'):
                    data.append(float(snmp_data.get('upsHighPrecBatteryActualVoltage.0', 0.0)) / 10)
                else:
                    data.append(None)
                                
                # Input voltage : upsHighPrecInputLineVoltage.0 = 1190
                if snmp_data.get('upsHighPrecInputLineVoltage.0'):
                    data.append(float(snmp_data.get('upsHighPrecInputLineVoltage.0', 0.0)) / 10)
                else:
                    data.append(None)
                
                # Input frequency : upsHighPrecOutputVoltage.0 = 1191
                if snmp_data.get('upsHighPrecInputFrequency.0'):
                    data.append(float(snmp_data.get('upsHighPrecInputFrequency.0', 0.0)) / 10)
                else:
                    data.append(None)
                
                # Output voltage : 1196
                if snmp_data.get('upsHighPrecOutputVoltage.0'):
                    data.append(float(snmp_data.get('upsHighPrecOutputVoltage.0', 0.0)) / 10)
                else:
                    data.append(None)
                
                # Output frequency : upsHighPrecOutputFrequency.0 = 600
                if snmp_data.get('upsHighPrecOutputFrequency.0'):
                    data.append(float(snmp_data.get('upsHighPrecOutputFrequency.0', 0.0)) / 10)
                else:
                    data.append(None)
                
                # Output load percent : upsHighPrecOutputLoad.0 = 68
                if snmp_data.get('upsHighPrecOutputLoad.0'):
                    data.append(float(snmp_data.get('upsHighPrecOutputLoad.0', 0.0)) / 10)
                else:
                    data.append(None)
                
                # Output current amps : upsHighPrecOutputCurrent.0 = 12
                if snmp_data.get('upsHighPrecOutputCurrent.0'):
                    data.append(float(snmp_data.get('upsHighPrecOutputCurrent.0', 0.0)) / 10)
                else:
                    data.append(None)
                
                # Output efficiency percent : upsHighPrecOutputEfficiency.0 = -2
                # This one is weird, it can go negative
                if snmp_data.get('upsHighPrecOutputEfficiency.0'):
                    data.append(max(0.0, float(snmp_data.get('upsHighPrecOutputEfficiency.0', 0.0)) / 10))
                else:
                    data.append(None)
                
                # Output energy usage kWh : upsHighPrecOutputEnergyUsage.0 = 340
                if snmp_data.get('upsHighPrecOutputEnergyUsage.0'):
                    data.append(float(snmp_data.get('upsHighPrecOutputEnergyUsage.0', 0.0)) / 100)
                else:
                    data.append(None)

                # Date parsing (usually "MM/DD/YYYY")
                manufacture_date = snmp_data.get('upsAdvIdentDateOfManufacture.0')
                if manufacture_date is not None:
                    try:
                        # Parse the date into a datetime.date object
                        if len(manufacture_date) > 8:
                            # upsAdvBatteryRecommendedReplaceDate.0 = "05/16/2027"
                            manufacture_date = datetime.datetime.strptime(manufacture_date, '%m/%d/%Y').date()
                        else:
                            # upsAdvBatteryRecommendedReplaceDate.0 = "03/07/22"
                            manufacture_date = datetime.datetime.strptime(manufacture_date, '%m/%d/%y').date()
                    except ValueError:
                        try:
                            # Parse the date into a datetime.date object 
                            manufacture_date = datetime.datetime.strptime(manufacture_date, '%m/%d/%Y').date()
                        except ValueError:
                            log.exception(f'Failed to parse upsAdvIdentDateOfManufacture.0 "{manufacture_date}"')
                            manufacture_date = None

                last_battery_replace_date = snmp_data.get('upsBasicBatteryLastReplaceDate.0')
                if last_battery_replace_date is not None:
                    try:
                        # Parse the date into a datetime.date object
                        if len(last_battery_replace_date) > 8:
                            # upsAdvBatteryRecommendedReplaceDate.0 = "05/16/2027"
                            last_battery_replace_date = datetime.datetime.strptime(last_battery_replace_date, '%m/%d/%Y').date()
                        else:
                            # upsAdvBatteryRecommendedReplaceDate.0 = "03/07/22"
                            last_battery_replace_date = datetime.datetime.strptime(last_battery_replace_date, '%m/%d/%y').date()
                    except ValueError:
                        log.exception(f'Failed to parse upsBasicBatteryLastReplaceDate.0 "{last_battery_replace_date}"')
                        last_battery_replace_date = None

                next_battery_replace_date = snmp_data.get('upsAdvBatteryRecommendedReplaceDate.0')
                if next_battery_replace_date is not None:
                    try:
                        # Parse the date into a datetime.date object
                        if len(next_battery_replace_date) > 8:
                            # upsAdvBatteryRecommendedReplaceDate.0 = "05/16/2027"
                            next_battery_replace_date = datetime.datetime.strptime(next_battery_replace_date, '%m/%d/%Y').date()
                        else:
                            # upsAdvBatteryRecommendedReplaceDate.0 = "03/07/22"
                            next_battery_replace_date = datetime.datetime.strptime(next_battery_replace_date, '%m/%d/%y').date()
                    except ValueError:
                        log.exception(f'Failed to parse upsAdvBatteryRecommendedReplaceDate.0 "{next_battery_replace_date}"')
                        next_battery_replace_date = None

                # Parse runtimes
                runtime_remaining = snmp_data.get('upsAdvBatteryRunTimeRemaining.0')
                if runtime_remaining is not None:
                    # upsAdvBatteryRunTimeRemaining.0 = 0:3:01:24.00 (days:hours:minutes:seconds)
                    days, hours, minutes, seconds = runtime_remaining.split(':')
                    runtime_remaining = (int(days) * 86400) + (int(hours) * 3600) + (int(minutes) * 60) + int(seconds[:2])
                
                on_battery = snmp_data.get('upsBasicBatteryTimeOnBattery.0')
                if on_battery is not None:
                    # upsBasicBatteryTimeOnBattery.0 = 0:0:00:00.00 (days:hours:minutes:seconds)
                    days, hours, minutes, seconds = on_battery.split(':')
                    on_battery = (int(days) * 86400) + (int(hours) * 3600) + (int(minutes) * 60) + int(seconds[:2])

                probes = {}
                sensor_name = []
                sensor_value = []

                # Battery temperature sensor : upsHighPrecExtdBatteryTemperature.0 = 206
                if snmp_data.get('upsHighPrecExtdBatteryTemperature.0') is not None:
                    sensor_name.append('Battery Temperature')
                    sensor_value.append(float(snmp_data['upsHighPrecExtdBatteryTemperature.0']) / 10)

                # Fetch probe data via SNMP 
                # SNMP probe data returns whole numbers only
                if ups['fetch_probes'] == 'snmp':
                    # Fetch the SNMP data
                    snmp_data = await self.fetch_snmp(
                        ip=ups['ip'],
                        version=ups['snmp_version'],
                        oids=probe_oid,
                        community=ups['snmp_community'],
                        username=ups['snmp_username'],
                        password=ups['snmp_password'],
                        port=ups['snmp_port'],
                        timeout=ups['timeout']
                    )
                    # Check if there was no data (failed to fetch)
                    if not snmp_data:
                        log.error(f'Failed to fetch SNMP probe data from UPS "{ups["name"]}" at IP "{ups["ip"]}"')
                    
                    # Parse the probe data
                    for probe in snmp_data.keys():
                        # Get the probe ID
                        probe_id = probe.split('.')[-1]
                        value = snmp_data[probe]
                        # Probe name
                        if probe.startswith('uioSensorStatusSensorName'):
                            probes[probe_id] = value
                        # Probe temperature
                        elif probe.startswith('uioSensorStatusTemperatureDegC'):
                            sensor_name.append(f'{probes[probe_id]} Temperature')
                            sensor_value.append(float(value))
                        # Probe humidity
                        elif probe.startswith('uioSensorStatusHumidity'):
                            sensor_name.append(f'{probes[probe_id]} Humidity')
                            sensor_value.append(float(value))

                # Fetch probe data via scraping uiostatus.htm
                # We get more precise data this way (0.5C increments)
                elif ups['fetch_probes'] in ('http', 'https'):
                    html = None
                    # Check if there's no NMC session
                    if ups['nmc_session'] is None:
                        # Try to generate a new NMC session
                        ups['nmc_session'] = await self.generate_nmc_session(ups)
                    else:
                        # We already have an NMC session
                        async with self.session.get(
                            f'{ups["fetch_probes"]}://{ups["ip"]}:{ups["http_port"]}/NMC/{ups["nmc_session"]}/uiostatus.htm',
                            timeout=ups['timeout']
                        ) as resp:
                            if resp.status != 200:
                                log.error(f'Failed to fetch HTTP probe data from UPS "{ups["name"]}" at IP "{ups["ip"]}": Got HTTP status {resp.status} {resp.reason}')
                            else:
                                html = await resp.text()

                    # Check if there's no HTML
                    # Assume the previous fetch failed (invalid NMC session?)
                    # Try to regenerate the NMC session and scrape again
                    if html is None and (session := await self.generate_nmc_session(ups)) is not None:
                        # Update the NMC session
                        ups['nmc_session'] = session
                        # Try to fetch again
                        async with self.session.get(
                            f'{ups["fetch_probes"]}://{ups["ip"]}:{ups["http_port"]}/NMC/{ups["nmc_session"]}/uiostatus.htm',
                            timeout=ups['timeout']
                        ) as resp:
                            if resp.status != 200:
                                log.error(f'Failed to refetch HTTP probe data from UPS "{ups["name"]}" at IP "{ups["ip"]}": Got HTTP status {resp.status} {resp.reason}')
                            else:
                                html = await resp.text()

                    # Check if we were able to scrape the page
                    if html:
                        probes = self.probe_html_regex.findall(html)
                        log.debug(f'Got HTML probes for {ups["ip"]} {probes}')

                        for probe in probes:
                            sensor_name.append(f'{probe[0]} Temperature')
                            # Check if the temperature is in Fahrenheit
                            if probe[2] == 'F':
                                # Convert to Celsius
                                sensor_value.append((float(probe[1]) - 32) * 5 / 9)
                            # Temperature is in Celsius
                            else:
                                sensor_value.append(float(probe[1]))
                            # Check if the probe has humidity data
                            if probe[3] != '':
                                sensor_name.append(f'{probe[0]} Humidity')
                                sensor_value.append(float(probe[3]))

                data.extend((
                    manufacture_date,
                    last_battery_replace_date,
                    next_battery_replace_date,
                    runtime_remaining,
                    on_battery,
                    sensor_name,
                    sensor_value,
                    timestamp
                ))

                self.clickhouse_queue.put_nowait(data)
            except Exception:
                log.exception(f'Failed to fetch UPS target "{ups["name"]}" at IP "{ups["ip"]}"')
            finally:
                # Wait the configured interval before fetching again
                await asyncio.sleep(ups['interval'] or self.fetch_interval)

    async def run(self):
        """
            Setup and run the exporter
        """
        # Load the UPS targets from targets.json
        self._load_targets()
        if not self.ups_targets:
            log.error('No valid UPS targets found in targets.json')
            exit(1)

        # Create a ClientSession that doesn't verify SSL certificates
        self.session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=False)
        )
        # Create the ClickHouse client
        self.clickhouse = aiochclient.ChClient(
            self.session,
            url=self.clickhouse_url,
            user=self.clickhouse_user,
            password=self.clickhouse_pass,
            database=self.clickhouse_db,
            json=json
        )
        log.debug(f'Using ClickHouse table "{self.clickhouse_table}" at "{self.clickhouse_url}"')

        # Run the queue inserter as a task
        asyncio.create_task(self.insert_to_clickhouse())

        for ups in self.ups_targets:
            # Run the fetcher as a task
            log.debug(f'Creating task for UPS {ups}')
            asyncio.create_task(self.fetch_ups(ups))

        # Run forever or until we get SIGTERM'd
        await self.stop_event.wait()

        log.info('Exiting...')
        # Close the ClientSession
        await self.session.close()
        # Close the ClickHouse client
        await self.clickhouse.close()


loop = asyncio.new_event_loop()
apc = APC(loop)

def sigterm_handler(_signo, _stack_frame):
    """
        Handle SIGTERM
    """
    # Set the event to stop the loop
    apc.stop_event.set()
# Register the SIGTERM handler
signal.signal(signal.SIGTERM, sigterm_handler)

loop.run_until_complete(apc.run())