#
#    Copyright (c) 2009-2023 Tom Keffer <tkeffer@gmail.com>
#
#    See the file LICENSE.txt for your full rights.
#
"""weedb driver for the PostgreSQL database"""

import psycopg2

from weeutil.weeutil import to_bool
import weedb

EXCEPTION_MAP = {
    '08000': weedb.CannotConnectError,
    '08001': weedb.CannotConnectError,
    '08003': weedb.CannotConnectError,
    '08004': weedb.CannotConnectError,
    '08006': weedb.CannotConnectError,
    '08007': weedb.CannotConnectError,
    '08P01': weedb.CannotConnectError,
    '23000': weedb.IntegrityError,
    '23001': weedb.IntegrityError,
    '23502': weedb.IntegrityError,
    '23503': weedb.IntegrityError,
    '23505': weedb.IntegrityError,
    '23514': weedb.IntegrityError,
    '23P01': weedb.IntegrityError,
    '28P01': weedb.BadPasswordError,
    '28000': weedb.PermissionError,
    '2F002': weedb.PermissionError,
    '2F003': weedb.PermissionError,
    '2F004': weedb.PermissionError,
    '38001': weedb.PermissionError,
    '38002': weedb.PermissionError,
    '38003': weedb.PermissionError,
    '38004': weedb.PermissionError,
    '42501': weedb.PermissionError,
    '42703': weedb.NoColumnError,
    '42P01': weedb.NoTableError,
    '42P04': weedb.DatabaseExistsError,
    '42P07': weedb.TableExistsError,
    '42P10': weedb.NoColumnError,
    None: weedb.DatabaseError
}
# PostgreSQL is case-insensitive, but WeeWX expects table names to be case-sensitive.
# This map links a case-insensitive name to a case-sensitive name.
# Hacky, but it works
COLUMNS_MAP = {
    'altimeter': 'altimeter',
    'apptemp': 'appTemp',
    'apptemp1': 'appTemp1',
    'barometer': 'barometer',
    'batterystatus1': 'batteryStatus1',
    'batterystatus2': 'batteryStatus2',
    'batterystatus3': 'batteryStatus3',
    'batterystatus4': 'batteryStatus4',
    'batterystatus5': 'batteryStatus5',
    'batterystatus6': 'batteryStatus6',
    'batterystatus7': 'batteryStatus7',
    'batterystatus8': 'batteryStatus8',
    'cloudbase': 'cloudbase',
    'co': 'co',
    'co2': 'co2',
    'consbatteryvoltage': 'consBatteryVoltage',
    'datetime': 'dateTime',
    'dewpoint': 'dewpoint',
    'dewpoint1': 'dewpoint1',
    'et': 'ET',
    'extrahumid1': 'extraHumid1',
    'extrahumid2': 'extraHumid2',
    'extrahumid3': 'extraHumid3',
    'extrahumid4': 'extraHumid4',
    'extrahumid5': 'extraHumid5',
    'extrahumid6': 'extraHumid6',
    'extrahumid7': 'extraHumid7',
    'extrahumid8': 'extraHumid8',
    'extratemp1': 'extraTemp1',
    'extratemp2': 'extraTemp2',
    'extratemp3': 'extraTemp3',
    'extratemp4': 'extraTemp4',
    'extratemp5': 'extraTemp5',
    'extratemp6': 'extraTemp6',
    'extratemp7': 'extraTemp7',
    'extratemp8': 'extraTemp8',
    'forecast': 'forecast',
    'hail': 'hail',
    'hailbatterystatus': 'hailBatteryStatus',
    'hailrate': 'hailRate',
    'heatindex': 'heatindex',
    'heatindex1': 'heatindex1',
    'heatingtemp': 'heatingTemp',
    'heatingvoltage': 'heatingVoltage',
    'humidex': 'humidex',
    'humidex1': 'humidex1',
    'indewpoint': 'inDewpoint',
    'inhumidity': 'inHumidity',
    'intemp': 'inTemp',
    'intempbatterystatus': 'inTempBatteryStatus',
    'interval': 'interval',
    'leaftemp1': 'leafTemp1',
    'leaftemp2': 'leafTemp2',
    'leafwet1': 'leafWet1',
    'leafwet2': 'leafWet2',
    'lightning_distance': 'lightning_distance',
    'lightning_disturber_count': 'lightning_disturber_count',
    'lightning_energy': 'lightning_energy',
    'lightning_noise_count': 'lightning_noise_count',
    'lightning_strike_count': 'lightning_strike_count',
    'luminosity': 'luminosity',
    'maxsolarrad': 'maxSolarRad',
    'nh3': 'nh3',
    'no2': 'no2',
    'noise': 'noise',
    'o3': 'o3',
    'outhumidity': 'outHumidity',
    'outtemp': 'outTemp',
    'outtempbatterystatus': 'outTempBatteryStatus',
    'pb': 'pb',
    'pm10_0': 'pm10_0',
    'pm1_0': 'pm1_0',
    'pm2_5': 'pm2_5',
    'pressure': 'pressure',
    'radiation': 'radiation',
    'rain': 'rain',
    'rainbatterystatus': 'rainBatteryStatus',
    'rainrate': 'rainRate',
    'referencevoltage': 'referenceVoltage',
    'rxcheckpercent': 'rxCheckPercent',
    'signal1': 'signal1',
    'signal2': 'signal2',
    'signal3': 'signal3',
    'signal4': 'signal4',
    'signal5': 'signal5',
    'signal6': 'signal6',
    'signal7': 'signal7',
    'signal8': 'signal8',
    'snow': 'snow',
    'snowbatterystatus': 'snowBatteryStatus',
    'snowdepth': 'snowDepth',
    'snowmoisture': 'snowMoisture',
    'snowrate': 'snowRate',
    'so2': 'so2',
    'soilmoist1': 'soilMoist1',
    'soilmoist2': 'soilMoist2',
    'soilmoist3': 'soilMoist3',
    'soilmoist4': 'soilMoist4',
    'soiltemp1': 'soilTemp1',
    'soiltemp2': 'soilTemp2',
    'soiltemp3': 'soilTemp3',
    'soiltemp4': 'soilTemp4',
    'supplyvoltage': 'supplyVoltage',
    'txbatterystatus': 'txBatteryStatus',
    'usunits': 'usUnits',
    'uv': 'UV',
    'uvbatterystatus': 'uvBatteryStatus',
    'windbatterystatus': 'windBatteryStatus',
    'windchill': 'windchill',
    'winddir': 'windDir',
    'windgust': 'windGust',
    'windgustdir': 'windGustDir',
    'windrun': 'windrun',
    'windspeed': 'windSpeed'
}


def guard(fn):
    """Decorator function that converts PostgreSQL exceptions into weedb exceptions."""

    def guarded_fn(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except psycopg2.Error as e:
            # Get the PostgreSQL exception number out of e:
            try:
                errno = e.pgcode
            except (IndexError, AttributeError):
                errno = None
            # Default exception is weedb.DatabaseError
            klass = EXCEPTION_MAP.get(errno, weedb.DatabaseError)
            raise klass(e)

    return guarded_fn


def connect(host='localhost', user='', password='', database_name='',
            driver='', port=5432, autocommit=True, **kwargs):
    """Connect to the specified database"""
    return Connection(host=host, port=int(port), user=user, password=password,
                      database_name=database_name, autocommit=autocommit, **kwargs)


def create(host='localhost', user='', password='', database_name='',
           driver='', port=5432, autocommit=True, **kwargs):
    """Create the specified database. If it already exists,
    an exception of type weedb.DatabaseExistsError will be raised."""

    # Open up a connection w/o specifying the database.
    with Connection(host=host,
                    port=int(port),
                    user=user,
                    password=password,
                    autocommit=autocommit,
                    **kwargs) as connect:
        with connect.cursor() as cursor:
            # Now create the database.
            cursor.execute('CREATE DATABASE %s' % (database_name,))


def drop(host='localhost', user='', password='', database_name='',
         driver='', port=5432, autocommit=True, **kwargs):
    """Drop (delete) the specified database."""

    with Connection(host=host,
                    port=int(port),
                    user=user,
                    password=password,
                    autocommit=autocommit,
                    **kwargs) as connect:
        with connect.cursor() as cursor:
            cursor.execute('DROP DATABASE %s' % (database_name,))


class Connection(weedb.Connection):
    """A wrapper around a PostgreSQL connection object."""
    # Override SQL statements used by the metadata in the daily summaries.
    meta_replace_str = ('INSERT INTO %s_day__metadata (name, value) VALUES (?, ?) '
                        'ON CONFLICT (name) '
                        'DO UPDATE SET (name, value) = (EXCLUDED.name, EXCLUDED.value)')
    sql_replace_str = ('INSERT INTO %s_day_%s VALUES (%s) '
                        'ON CONFLICT (dateTime) '
                        'DO UPDATE SET (min, mintime, max, maxtime, sum, count, wsum, sumtime) = '
                        '(EXCLUDED.min, EXCLUDED.mintime, EXCLUDED.max, EXCLUDED.maxtime, '
                        'EXCLUDED.sum, EXCLUDED.count, EXCLUDED.wsum, EXCLUDED.sumtime)')

    @guard
    def __init__(self, host='localhost', user='', password='', database_name='',
                 port=5432, autocommit=True, **kwargs):
        """Initialize an instance of Connection.

        Args:
            host (str): IP or hostname hosting the mysql database.
                Alternatively, the path to the socket mount. (required)
            user (str): The username (required)
            password (str): The password for the username (required)
            database_name (str): The database to be used. (required)
            port (int): Its port number (optional; default is 5432)
            autocommit (bool): If True, autocommit is enabled (default is True)
            kwargs (dict):   Any extra arguments you may wish to pass on to PostgreSQL
              connect statement.
        """
        connection = psycopg2.connect(host=host, port=int(port), user=user, password=password,
                                      dbname=database_name, **kwargs)

        weedb.Connection.__init__(self, connection, database_name, 'postgres')

        # Set the transaction isolation level.
        self.connection.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED
        self.connection.autocommit = to_bool(autocommit)

    def cursor(self):
        """Return a cursor object."""
        return Cursor(self)

    @guard
    def tables(self):
        """Returns a list of tables in the database."""

        def _convert(table_name):
            # This is a bit of a hack, due to WeeWX expecting case-sensitive names
            table_suffix = table_name.split('_')[-1]
            if new_suffix := COLUMNS_MAP.get(table_suffix, table_suffix):
                return table_name.replace(table_suffix, new_suffix)
            return table_name

        with self.connection.cursor() as cursor:
            cursor.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
            )
            return [_convert(row[0]) for row in cursor.fetchall()]

    @guard
    def genSchemaOf(self, table):
        """Return a summary of the schema of the specified table.

        If the table does not exist, an exception of type weedb.OperationalError is raised."""

        with self.connection.cursor() as cursor:
            # If the table does not exist, this will raise a PostgreSQL ProgrammingError exception,
            # which gets converted to a weedb.OperationalError exception by the guard decorator
            cursor.execute("""
                SELECT
                  c.column_name,
                  c.data_type,
                  c.is_nullable,
                  EXISTS(
                    SELECT
                      t.constraint_type
                    FROM
                      information_schema.key_column_usage AS k
                      JOIN information_schema.table_constraints AS t ON t.constraint_name = k.constraint_name
                    WHERE k.column_name = c.column_name AND k.table_name = c.table_name
                  ) AS is_primary,
                  c.column_default
                FROM
                  information_schema.columns AS c
                WHERE
                  c.table_name = %s;
                """, (table,)
            )
            for idx, row in enumerate(cursor.fetchall()):
                if 'CHAR' in row[1].upper():
                    type_ = 'STR'
                else:
                    type_ = str(row[1]).upper()
                yield idx, COLUMNS_MAP.get(row[0], row[0]), type_, row[4], row[3]

    @guard
    def columnsOf(self, table):
        """Return a list of columns in the specified table.

        If the table does not exist, an exception of type weedb.OperationalError is raised."""
        with self.connection.cursor() as cursor:
            cursor.execute('SELECT column_name FROM information_schema.columns WHERE table_name = %s',
                           (table,))
            return [COLUMNS_MAP.get(row[0], row[0]) for row in cursor.fetchall()]

    @guard
    def get_variable(self, var_name):
        with self.connection.cursor() as cursor:
            cursor.execute('SELECT name, setting FROM pg_settings WHERE name = %s', (var_name,))
            row = cursor.fetchone()
            # This is actually a 2-way tuple (variable-name, variable-value),
            # or None, if the variable does not exist.
            return row

    @guard
    def begin(self):
        """Begin a transaction."""
        # The psycopg2 driver automatically starts commits, so this can just be ignored
        pass

    @guard
    def commit(self):
        self.connection.commit()

    @guard
    def rollback(self):
        self.connection.rollback()


class Cursor(weedb.Cursor):
    """A wrapper around the psycopg2 cursor object"""

    @guard
    def __init__(self, connection):
        """Initialize a Cursor from a connection.

        connection: An instance of db.mysql.Connection"""

        # Get the PostgreSQLdb cursor and store it internally:
        self.cursor = connection.connection.cursor()

    @guard
    def execute(self, sql_string, sql_tuple=()):
        """Execute a SQL statement on the PostgreSQL server.

        sql_string: A SQL statement to be executed. It should use ? as
        a placeholder.

        sql_tuple: A tuple with the values to be used in the placeholders."""
        # PostgreSQL uses '%s' as placeholders, so replace the ?'s with %s
        postgres_string = sql_string.replace('?', '%s')
        # Convert sql_tuple to a plain old tuple, just in case it actually
        # derives from tuple, but overrides the string conversion (as is the
        # case with a TimeSpan object):
        self.cursor.execute(postgres_string, tuple(sql_tuple))
        return self

    def fetchone(self):
        # Get a result from the PostgreSQL cursor
        return self.cursor.fetchone()

    def drop_columns(self, table, column_names):
        """Drop the set of 'column_names' from table 'table'.

        table: The name of the table from which the column(s) are to be dropped.

        column_names: A set (or list) of column names to be dropped. It is not an error to try to drop
        a non-existent column.
        """
        for column_name in column_names:
            self.execute('ALTER TABLE %s DROP COLUMN %s;' % (table, column_name))

    def close(self):
        try:
            self.cursor.close()
            del self.cursor
        except AttributeError:
            pass

    def __iter__(self):
        return self

    def __next__(self):
        result = self.fetchone()
        if result is None:
            raise StopIteration
        return result

    def __enter__(self):
        return self

    def __exit__(self, etyp, einst, etb):
        self.close()
