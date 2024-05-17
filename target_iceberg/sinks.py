"""iceberg target sink class, which handles writing streams."""

from __future__ import annotations

from pyiceberg.schema import Schema
from pyiceberg.types import *

from singer_sdk.sinks import BatchSink
import pyarrow as pa
from decimal import Decimal

from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError, NoSuchNamespaceError, NamespaceAlreadyExistsError


class icebergSink(BatchSink):
    """iceberg target sink class."""

    def __init__(self, target, schema, stream_name, key_properties) -> None:
        super().__init__(
            target=target,
            schema=schema,
            stream_name=stream_name,
            key_properties=key_properties,
        )
        # Accessing the properties from the target's config
        self.catalog = None
        self.pyarrow_schema = None
        self.pyiceberg_schema = None
        self.rows = None
        self.table_instance = None
        self.aws_access_key = self.config.get("aws_access_key")
        self.aws_secret_key = self.config.get("aws_secret_key")
        self.aws_region = self.config.get("aws_region", None)
        self.s3_endpoint = self.config.get("s3_endpoint")
        self.hive_thrift_uri = self.config.get("hive_thrift_uri")
        self.warehouse_uri = self.config.get("warehouse_uri")
        self.default_decimal_precision = self.config.get("decimal_precision")
        self.default_decimal_scale = self.config.get("decimal_scale")
        self.database = self.config.get('database', None)
        self.table_exists_policy = self.config.get('table_exists')

    def start_batch(self, context: dict) -> None:
        """Start a batch.

        Developers may optionally add additional markers to the `context` dict,
        which is unique to this batch.

        Args:
            context: Stream partition or context dictionary.
        """
        batch_key = context["batch_id"]
        self.rows = []

    def process_record(self, record: dict, context: dict) -> None:
        """Process the record.

        Developers may optionally read or write additional markers within the
        passed `context` dict from the current batch.

        Args:
            record: Individual record in the stream.
            context: Stream partition or context dictionary.
        """
        self.rows.append(record)

    def create_dataframe(self, record: list):
        return pa.Table.from_pylist(record, schema=self.pyarrow_schema)

    def init_pyiceberg(self):
        self.catalog = load_catalog(
            "default",
            **{
                "uri": f"{self.hive_thrift_uri}",
                "s3.endpoint": f"{self.s3_endpoint}",
                "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
                "s3.access-key-id": f"{self.aws_access_key}",
                "s3.secret-access-key": f"{self.aws_secret_key}",
            }
        )

    def validate_database(self):
        try:
            self.catalog.load_namespace_properties(self.database)
            namespace_exists = True
        except NoSuchNamespaceError:
            namespace_exists = False
        return namespace_exists

    def create_database(self):
        try:
            self.catalog.create_namespace(self.database, {'location': f'{self.warehouse_uri}/{self.database}'})
            self.logger.info(f"Namespace {self.database} was created.")
        except NamespaceAlreadyExistsError:
            self.logger.debug(f"Namespace {self.database} was already exists,create was skipped.")

    def format_data_schema(self):
        schema_data = self.schema.get("properties")
        pyarrow_schema_column_list = []
        for column_name, column_type in schema_data.items():
            field_format = column_type.get('format', None)
            if column_name in self.key_properties:
                field_required = True
            else:
                field_required = False
            field_type_list = column_type.get('type')
            field_multiple_of = column_type.get('multipleOf', None)
            pyarrow_type_instance = None
            if 'integer' in field_type_list:
                pyarrow_type_instance = pa.int64()
            elif 'boolean' in field_type_list:
                pyarrow_type_instance = pa.bool_()
            elif 'number' in field_type_list:
                if field_multiple_of:
                    scale = abs(Decimal(str(field_multiple_of)).as_tuple().exponent)
                    pyarrow_type_instance = pa.decimal128(self.default_decimal_precision, scale)
                else:
                    pyarrow_type_instance = pa.decimal128(self.default_decimal_precision, self.default_decimal_scale)
            elif 'string' in field_type_list and field_format == 'date-time':
                pyarrow_type_instance = pa.timestamp('us')
            elif 'string' in field_type_list and field_format == 'date':
                pyarrow_type_instance = pa.date64()
            elif 'string' in field_type_list and field_format == 'time':
                pyarrow_type_instance = pa.time64('us')
            elif 'string' in field_type_list:
                pyarrow_type_instance = pa.string()
            else:
                self.logger.error(f"Unsupported type {field_type_list} {field_format}")
            pyarrow_schema_column_list.append(
                pa.field(column_name,
                         pyarrow_type_instance,
                         nullable=not field_required))
        self.pyarrow_schema = pa.schema(
            pyarrow_schema_column_list,
        )
        self.logger.debug(f"data schema  {self.pyarrow_schema}.")

    def create_or_load_table(self):
        try:
            self.table_instance = self.catalog.load_table((self.database, self.stream_name))
            self.logger.info(f"table {self.database}.{self.stream_name} was loaded.")
            return True
        except NoSuchTableError:
            schema_data = self.schema.get("properties")
            pyiceberg_schema_column_list = []
            column_num = 1
            identifier_field_ids = []
            for column_name, column_type in schema_data.items():
                field_format = column_type.get('format', None)
                field_multiple_of = column_type.get('multipleOf', None)

                if column_name in self.key_properties:
                    field_required = True
                    identifier_field_ids.append(column_num)
                else:
                    field_required = False
                field_type_list = column_type.get('type')
                pyiceberg_type_instance = None
                if 'integer' in field_type_list:
                    pyiceberg_type_instance = LongType()
                elif 'boolean' in field_type_list:
                    pyiceberg_type_instance = BooleanType()
                elif 'number' in field_type_list:
                    if field_multiple_of:
                        scale = abs(Decimal(str(field_multiple_of)).as_tuple().exponent)
                        pyiceberg_type_instance = DecimalType(self.default_decimal_precision, scale)
                    else:
                        pyiceberg_type_instance = DecimalType(self.default_decimal_precision,
                                                              self.default_decimal_scale)
                elif 'string' in field_type_list and field_format == 'date-time':
                    pyiceberg_type_instance = TimestampType()
                elif 'string' in field_type_list and field_format == 'date':
                    pyiceberg_type_instance = DateType()
                elif 'string' in field_type_list and field_format == 'time':
                    pyiceberg_type_instance = TimeType()
                elif 'string' in field_type_list:
                    pyiceberg_type_instance = StringType()
                else:
                    self.logger.error(f"Unsupported type {field_type_list}")

                pyiceberg_schema_column_list.append(
                    NestedField(field_id=column_num,
                                name=column_name,
                                field_type=pyiceberg_type_instance,
                                required=field_required))

                column_num += 1

            self.pyiceberg_schema = Schema(
                *pyiceberg_schema_column_list,
                identifier_field_ids=identifier_field_ids,
            )

            self.table_instance = self.catalog.create_table(identifier=f"{self.database}.{self.stream_name}",
                                                            schema=self.pyiceberg_schema)
            self.logger.debug(f"table {self.database}.{self.stream_name}  {self.pyiceberg_schema}.")
            self.logger.info(f"table {self.database}.{self.stream_name} was created.")

            return False
        except Exception as e:
            self.logger.error(e)

    def write_data(self, df, table_exists):

        if table_exists:
            if self.table_exists_policy == 'append':
                self.table_instance.append(df)
                self.logger.info(f"table {self.database}.{self.stream_name} append  successfully written")
            elif self.table_exists_policy == 'overwrite':
                self.table_instance.overwrite(df)
                self.logger.info(f"table {self.database}.{self.stream_name} overwrite  successfully written")
            elif self.table_exists_policy == 'replace':
                self.catalog.drop_table((self.database, self.stream_name))
                self.logger.info(f"table {self.database}.{self.stream_name}  has been deleted.")
                table_exists = self.create_or_load_table()
                if not table_exists:
                    self.table_instance.append(df)
                    self.logger.info(f"table {self.database}.{self.stream_name} replace  successfully written")
            elif self.table_exists_policy == 'skip':
                self.logger.info(f"Because the parameter table  {self.database}.{self.stream_name}  was skipped ")
            else:
                self.logger.error(f"Unsupported table exists policy {self.table_exists_policy}")
        else:
            self.table_instance.append(df)
            self.logger.info(f"table {self.database}.{self.stream_name} append  successfully written")

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written.

        Args:
            context: Stream partition or context dictionary.
        """

        self.init_pyiceberg()
        if not self.database:
            self.database = self.stream_name.split('-')[0]
            self.stream_name = self.stream_name.split('-')[1]
        if not self.validate_database():
            self.create_database()
        table_exists = self.create_or_load_table()
        self.format_data_schema()
        df = self.create_dataframe(self.rows)
        self.write_data(df, table_exists)
