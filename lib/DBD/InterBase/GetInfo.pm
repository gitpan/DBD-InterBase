#   $Id: GetInfo.pm,v 1.2 2002/08/02 07:21:53 edpratomo Exp $
#
#   Copyright (c) 2002 Edwin Pratomo
#
#   You may distribute under the terms of either the GNU General Public
#   License or the Artistic License, as specified in the Perl README file,
#   with the exception that it cannot be placed on a CD-ROM or similar media
#   for commercial distribution without the prior approval of the author.

package DBD::InterBase::GetInfo;


use DBD::InterBase();

my $ver_fmt = '%02d.%02d.%04d';   # ODBC version string: ##.##.#####

my $sql_driver_ver = sprintf $ver_fmt, split (/\./, $DBD::InterBase::VERSION);

my @Keywords = qw(
ACTION
ACTIVE
ADD
ADMIN
AFTER
ALL
ALTER
AND
ANY
AS
ASC
ASCENDING
AT
AUTO
AVG
BASE_NAME
BEFORE
BEGIN
BETWEEN
BLOB
BY
CACHE
CASCADE
CAST
CHAR
CHARACTER
CHECK
CHECK_POINT_LENGTH
COLLATE
COLUMN
COMMIT
COMMITTED
COMPUTED
CONDITIONAL
CONSTRAINT
CONTAINING
COUNT
CREATE
CSTRING
CURRENT
CURRENT_DATE
CURRENT_TIME
CURRENT_TIMESTAMP
CURSOR
DATABASE
DATE
DAY
DEBUG
DEC
DECIMAL
DECLARE
DEFAULT
DELETE
DESC
DESCENDING
DISTINCT
DO
DOMAIN
DOUBLE
DROP
ELSE
END
ENTRY_POINT
ESCAPE
EXCEPTION
EXECUTE
EXISTS
EXIT
EXTERNAL
EXTRACT
FILE
FILTER
FLOAT
FOR
FOREIGN
FREE_IT
FROM
FULL
FUNCTION
GDSCODE
GENERATOR
GEN_ID
GRANT
GROUP
GROUP_COMMIT_WAIT_TIME
HAVING
HOUR
IF
IN
INACTIVE
INDEX
INNER
INPUT_TYPE
INSERT
INT
INTEGER
INTO
IS
ISOLATION
JOIN
KEY
LEFT
LENGTH
LEVEL
LIKE
LOGFILE
LOG_BUFFER_SIZE
LONG
MANUAL
MAX
MAXIMUM_SEGMENT
MERGE
MESSAGE
MIN
MINUTE
MODULE_NAME
MONTH
NAMES
NATIONAL
NATURAL
NCHAR
NO
NOT
NULL
NUMERIC
NUM_LOG_BUFFERS
OF
ON
ONLY
OPTION
OR
ORDER
OUTER
OUTPUT_TYPE
OVERFLOW
PAGE
PAGES
PAGE_SIZE
PARAMETER
PASSWORD
PLAN
POSITION
POST_EVENT
PRECISION
PRIMARY
PRIVILEGES
PROCEDURE
PROTECTED
RAW_PARTITIONS
RDB$DB_KEY
READ
REAL
RECORD_VERSION
REFERENCES
RESERV
RESERVINGRESTRICT
RETAIN
RETURNING_VALUES
RETURNS
REVOKE
RIGHT
ROLE
ROLLBACK
SCHEMA
SECOND
SEGMENT
SELECT
SET
SHADOW
SHARED
SINGULAR
SIZE
SMALLINT
SNAPSHOT
SOME
SORT
SQLCODE
STABILITY
STARTING
STARTS
STATISTICS
SUB_TYPE
SUM
SUSPEND
TABLE
THEN
TIME
TIMESTAMP
TO
TRANSACTION
TRIGGER
TYPE
UNCOMMITTED
UNION
UNIQUE
UPDATE
UPPER
USER
VALUE
VALUES
VARCHAR
VARIABLE
VARYING
VIEW
WAIT
WEEKDAY
WHEN
WHERE
WHILE
WITH
WORK
WRITE
YEAR
YEARDA
);

sub sql_data_source_name {
    my $dbh = shift;
    return 'dbi:InterBase:' . $dbh->{Name};
}

sub sql_dbms_version {
	shift->func('version', 'ib_database_info')->{version};
}

sub sql_keywords {
    return join ',', @Keywords;
}
sub sql_user_name {
    my $dbh = shift;
    return $dbh->{CURRENT_USER};
}

%info = (
     20 => 'Y'                             # SQL_ACCESSIBLE_PROCEDURES
,    19 => 'Y'                             # SQL_ACCESSIBLE_TABLES
,     0 => 0                               # SQL_ACTIVE_CONNECTIONS
,   116 => 0                               # SQL_ACTIVE_ENVIRONMENTS
,     1 => 0                               # SQL_ACTIVE_STATEMENTS
,   169 => 127                             # SQL_AGGREGATE_FUNCTIONS
,   117 => 0                               # SQL_ALTER_DOMAIN
,    86 => 134763                          # SQL_ALTER_TABLE
, 10021 => 0                               # SQL_ASYNC_MODE
,   120 => 0                               # SQL_BATCH_ROW_COUNT
,   121 => 0                               # SQL_BATCH_SUPPORT
,    82 => 0                               # SQL_BOOKMARK_PERSISTENCE
,   114 => 1                               # SQL_CATALOG_LOCATION
, 10003 => 'N'                             # SQL_CATALOG_NAME
,    41 => ''                              # SQL_CATALOG_NAME_SEPARATOR
,    42 => ''                              # SQL_CATALOG_TERM
,    92 => 0                               # SQL_CATALOG_USAGE
, 10004 => 'ISO 8859-1'                    # SQL_COLLATING_SEQUENCE
, 10004 => 'ISO 8859-1'                    # SQL_COLLATION_SEQ
,    87 => 'Y'                             # SQL_COLUMN_ALIAS
,    22 => 0                               # SQL_CONCAT_NULL_BEHAVIOR
,    53 => 0                               # SQL_CONVERT_BIGINT
,    54 => 0                               # SQL_CONVERT_BINARY
,    55 => 0                               # SQL_CONVERT_BIT
,    56 => 0                               # SQL_CONVERT_CHAR
,    57 => 0                               # SQL_CONVERT_DATE
,    58 => 0                               # SQL_CONVERT_DECIMAL
,    59 => 0                               # SQL_CONVERT_DOUBLE
,    60 => 0                               # SQL_CONVERT_FLOAT
,    48 => 0                               # SQL_CONVERT_FUNCTIONS
#   173 => undef                           # SQL_CONVERT_GUID
,    61 => 0                               # SQL_CONVERT_INTEGER
,   123 => 0                               # SQL_CONVERT_INTERVAL_DAY_TIME
,   124 => 0                               # SQL_CONVERT_INTERVAL_YEAR_MONTH
,    71 => 0                               # SQL_CONVERT_LONGVARBINARY
,    62 => 0                               # SQL_CONVERT_LONGVARCHAR
,    63 => 0                               # SQL_CONVERT_NUMERIC
,    64 => 0                               # SQL_CONVERT_REAL
,    65 => 0                               # SQL_CONVERT_SMALLINT
,    66 => 0                               # SQL_CONVERT_TIME
,    67 => 0                               # SQL_CONVERT_TIMESTAMP
,    68 => 0                               # SQL_CONVERT_TINYINT
,    69 => 0                               # SQL_CONVERT_VARBINARY
,    70 => 0                               # SQL_CONVERT_VARCHAR
,   122 => 0                               # SQL_CONVERT_WCHAR
#   125 => undef                           # SQL_CONVERT_WLONGVARCHAR
#   126 => undef                           # SQL_CONVERT_WVARCHAR
,    74 => 2                               # SQL_CORRELATION_NAME
,   127 => 0                               # SQL_CREATE_ASSERTION
,   128 => 0                               # SQL_CREATE_CHARACTER_SET
,   129 => 0                               # SQL_CREATE_COLLATION
,   130 => 0                               # SQL_CREATE_DOMAIN
,   131 => 0                               # SQL_CREATE_SCHEMA
,   132 => 4609                            # SQL_CREATE_TABLE
,   133 => 0                               # SQL_CREATE_TRANSLATION
,   134 => 1                               # SQL_CREATE_VIEW
,    23 => 2                               # SQL_CURSOR_COMMIT_BEHAVIOR
,    24 => 2                               # SQL_CURSOR_ROLLBACK_BEHAVIOR
, 10001 => 0                               # SQL_CURSOR_SENSITIVITY
,     2 => \&sql_data_source_name          # SQL_DATA_SOURCE_NAME
,    25 => 'N'                             # SQL_DATA_SOURCE_READ_ONLY
,   119 => 7                               # SQL_DATETIME_LITERALS
,    17 => 'InterBase'                     # SQL_DBMS_NAME
,    18 => \&sql_dbms_version              # SQL_DBMS_VER
,    18 => \&sql_dbms_version              # SQL_DBMS_VERSION
,   170 => 3                               # SQL_DDL_INDEX
,    26 => 2                               # SQL_DEFAULT_TRANSACTION_ISOLATION
,    26 => 2                               # SQL_DEFAULT_TXN_ISOLATION
, 10002 => 'Y'                             # SQL_DESCRIBE_PARAMETER
,   171 => '03.51.0002.0000'               # SQL_DM_VER
,     3 => 136524976                       # SQL_DRIVER_HDBC
#   135 => undef                           # SQL_DRIVER_HDESC
,     4 => 136524904                       # SQL_DRIVER_HENV
#    76 => undef                           # SQL_DRIVER_HLIB
#     5 => undef                           # SQL_DRIVER_HSTMT
,     6 => 'libib6odbc.so'                 # SQL_DRIVER_NAME
,    77 => '03.51'                         # SQL_DRIVER_ODBC_VER
,     7 => $sql_driver_ver                 # SQL_DRIVER_VER
,   136 => 0                               # SQL_DROP_ASSERTION
,   137 => 0                               # SQL_DROP_CHARACTER_SET
,   138 => 0                               # SQL_DROP_COLLATION
,   139 => 0                               # SQL_DROP_DOMAIN
,   140 => 0                               # SQL_DROP_SCHEMA
,   141 => 7                               # SQL_DROP_TABLE
,   142 => 0                               # SQL_DROP_TRANSLATION
,   143 => 1                               # SQL_DROP_VIEW
,   144 => 0                               # SQL_DYNAMIC_CURSOR_ATTRIBUTES1
,   145 => 0                               # SQL_DYNAMIC_CURSOR_ATTRIBUTES2
,    27 => 'N'                             # SQL_EXPRESSIONS_IN_ORDERBY
,     8 => 1                               # SQL_FETCH_DIRECTION
,    84 => 0                               # SQL_FILE_USAGE
,   146 => 60417                           # SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1
,   147 => 3                               # SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2
,    81 => 11                              # SQL_GETDATA_EXTENSIONS
,    88 => 3                               # SQL_GROUP_BY
,    28 => 1                               # SQL_IDENTIFIER_CASE
,    29 => '"'                             # SQL_IDENTIFIER_QUOTE_CHAR
,   148 => 3                               # SQL_INDEX_KEYWORDS
,   149 => 0                               # SQL_INFO_SCHEMA_VIEWS
,   172 => 7                               # SQL_INSERT_STATEMENT
,    73 => 'Y'                             # SQL_INTEGRITY
,   150 => 0                               # SQL_KEYSET_CURSOR_ATTRIBUTES1
,   151 => 0                               # SQL_KEYSET_CURSOR_ATTRIBUTES2
,    89 => \&sql_keywords                  # SQL_KEYWORDS
,   113 => 'Y'                             # SQL_LIKE_ESCAPE_CLAUSE
,    78 => 0                               # SQL_LOCK_TYPES
,    34 => 0                               # SQL_MAXIMUM_CATALOG_NAME_LENGTH
,    97 => 0                               # SQL_MAXIMUM_COLUMNS_IN_GROUP_BY
,    98 => 0                               # SQL_MAXIMUM_COLUMNS_IN_INDEX
,    99 => 0                               # SQL_MAXIMUM_COLUMNS_IN_ORDER_BY
,   100 => 0                               # SQL_MAXIMUM_COLUMNS_IN_SELECT
,   101 => 0                               # SQL_MAXIMUM_COLUMNS_IN_TABLE
,    30 => 32                              # SQL_MAXIMUM_COLUMN_NAME_LENGTH
,     1 => 0                               # SQL_MAXIMUM_CONCURRENT_ACTIVITIES
,    31 => 18                              # SQL_MAXIMUM_CURSOR_NAME_LENGTH
,     0 => 0                               # SQL_MAXIMUM_DRIVER_CONNECTIONS
, 10005 => 32                              # SQL_MAXIMUM_IDENTIFIER_LENGTH
,   102 => 0                               # SQL_MAXIMUM_INDEX_SIZE
,   104 => 0                               # SQL_MAXIMUM_ROW_SIZE
,    32 => 0                               # SQL_MAXIMUM_SCHEMA_NAME_LENGTH
,   105 => 0                               # SQL_MAXIMUM_STATEMENT_LENGTH
# 20000 => undef                           # SQL_MAXIMUM_STMT_OCTETS
# 20001 => undef                           # SQL_MAXIMUM_STMT_OCTETS_DATA
# 20002 => undef                           # SQL_MAXIMUM_STMT_OCTETS_SCHEMA
,   106 => 0                               # SQL_MAXIMUM_TABLES_IN_SELECT
,    35 => 32                              # SQL_MAXIMUM_TABLE_NAME_LENGTH
,   107 => 128                             # SQL_MAXIMUM_USER_NAME_LENGTH
, 10022 => 0                               # SQL_MAX_ASYNC_CONCURRENT_STATEMENTS
,   112 => 0                               # SQL_MAX_BINARY_LITERAL_LEN
,    34 => 0                               # SQL_MAX_CATALOG_NAME_LEN
,   108 => 0                               # SQL_MAX_CHAR_LITERAL_LEN
,    97 => 0                               # SQL_MAX_COLUMNS_IN_GROUP_BY
,    98 => 0                               # SQL_MAX_COLUMNS_IN_INDEX
,    99 => 0                               # SQL_MAX_COLUMNS_IN_ORDER_BY
,   100 => 0                               # SQL_MAX_COLUMNS_IN_SELECT
,   101 => 0                               # SQL_MAX_COLUMNS_IN_TABLE
,    30 => 32                              # SQL_MAX_COLUMN_NAME_LEN
,     1 => 0                               # SQL_MAX_CONCURRENT_ACTIVITIES
,    31 => 18                              # SQL_MAX_CURSOR_NAME_LEN
,     0 => 0                               # SQL_MAX_DRIVER_CONNECTIONS
, 10005 => 32                              # SQL_MAX_IDENTIFIER_LEN
,   102 => 0                               # SQL_MAX_INDEX_SIZE
,    32 => 0                               # SQL_MAX_OWNER_NAME_LEN
,    33 => 32                              # SQL_MAX_PROCEDURE_NAME_LEN
,    34 => 0                               # SQL_MAX_QUALIFIER_NAME_LEN
,   104 => 0                               # SQL_MAX_ROW_SIZE
,   103 => 'N'                             # SQL_MAX_ROW_SIZE_INCLUDES_LONG
,    32 => 0                               # SQL_MAX_SCHEMA_NAME_LEN
,   105 => 0                               # SQL_MAX_STATEMENT_LEN
,   106 => 0                               # SQL_MAX_TABLES_IN_SELECT
,    35 => 32                              # SQL_MAX_TABLE_NAME_LEN
,   107 => 128                             # SQL_MAX_USER_NAME_LEN
,    37 => 'N'                             # SQL_MULTIPLE_ACTIVE_TXN
,    36 => 'N'                             # SQL_MULT_RESULT_SETS
,   111 => 'N'                             # SQL_NEED_LONG_DATA_LEN
,    75 => 1                               # SQL_NON_NULLABLE_COLUMNS
,    85 => 0                               # SQL_NULL_COLLATION
,    49 => 0                               # SQL_NUMERIC_FUNCTIONS
,     9 => 2                               # SQL_ODBC_API_CONFORMANCE
,   152 => 3                               # SQL_ODBC_INTERFACE_CONFORMANCE
#    12 => undef                           # SQL_ODBC_SAG_CLI_CONFORMANCE
,    15 => 2                               # SQL_ODBC_SQL_CONFORMANCE
,    73 => 'Y'                             # SQL_ODBC_SQL_OPT_IEF
,    10 => '03.51'                         # SQL_ODBC_VER
,   115 => 127                             # SQL_OJ_CAPABILITIES
,    90 => 'N'                             # SQL_ORDER_BY_COLUMNS_IN_SELECT
,    38 => 'Y'                             # SQL_OUTER_JOINS
,   115 => 127                             # SQL_OUTER_JOIN_CAPABILITIES
,    39 => ''                              # SQL_OWNER_TERM
,    91 => 0                               # SQL_OWNER_USAGE
,   153 => 0                               # SQL_PARAM_ARRAY_ROW_COUNTS
,   154 => 0                               # SQL_PARAM_ARRAY_SELECTS
,    80 => 7                               # SQL_POSITIONED_STATEMENTS
,    79 => 0                               # SQL_POS_OPERATIONS
,    21 => 'Y'                             # SQL_PROCEDURES
,    40 => 'PROCEDURE'                     # SQL_PROCEDURE_TERM
,   114 => 1                               # SQL_QUALIFIER_LOCATION
,    41 => ''                              # SQL_QUALIFIER_NAME_SEPARATOR
,    42 => ''                              # SQL_QUALIFIER_TERM
,    92 => 0                               # SQL_QUALIFIER_USAGE
,    93 => 3                               # SQL_QUOTED_IDENTIFIER_CASE
,    11 => 'N'                             # SQL_ROW_UPDATES
,    39 => ''                              # SQL_SCHEMA_TERM
,    91 => 0                               # SQL_SCHEMA_USAGE
,    43 => 10                              # SQL_SCROLL_CONCURRENCY
,    44 => 17                              # SQL_SCROLL_OPTIONS
,    14 => '\\'                            # SQL_SEARCH_PATTERN_ESCAPE
,    13 => 'INTERBASE'                     # SQL_SERVER_NAME
,    94 => ' $'                            # SQL_SPECIAL_CHARACTERS
,   155 => 7                               # SQL_SQL92_DATETIME_FUNCTIONS
,   156 => 15                              # SQL_SQL92_FOREIGN_KEY_DELETE_RULE
,   157 => 15                              # SQL_SQL92_FOREIGN_KEY_UPDATE_RULE
,   158 => 8176                            # SQL_SQL92_GRANT
,   159 => 63                              # SQL_SQL92_NUMERIC_VALUE_FUNCTIONS
,   160 => 16007                           # SQL_SQL92_PREDICATES
,   161 => 984                             # SQL_SQL92_RELATIONAL_JOIN_OPERATORS
,   162 => 32160                           # SQL_SQL92_REVOKE
,   163 => 15                              # SQL_SQL92_ROW_VALUE_CONSTRUCTOR
,   164 => 233                             # SQL_SQL92_STRING_FUNCTIONS
,   165 => 15                              # SQL_SQL92_VALUE_EXPRESSIONS
,   118 => 1                               # SQL_SQL_CONFORMANCE
,   166 => 3                               # SQL_STANDARD_CLI_CONFORMANCE
,   167 => 0                               # SQL_STATIC_CURSOR_ATTRIBUTES1
,   168 => 0                               # SQL_STATIC_CURSOR_ATTRIBUTES2
,    83 => 0                               # SQL_STATIC_SENSITIVITY
,    50 => 0                               # SQL_STRING_FUNCTIONS
,    95 => 31                              # SQL_SUBQUERIES
,    51 => 7                               # SQL_SYSTEM_FUNCTIONS
,    45 => 'TABLE'                         # SQL_TABLE_TERM
,   109 => 511                             # SQL_TIMEDATE_ADD_INTERVALS
,   110 => 511                             # SQL_TIMEDATE_DIFF_INTERVALS
,    52 => 0                               # SQL_TIMEDATE_FUNCTIONS
,    46 => 2                               # SQL_TRANSACTION_CAPABLE
,    72 => 14                              # SQL_TRANSACTION_ISOLATION_OPTION
,    46 => 2                               # SQL_TXN_CAPABLE
,    72 => 14                              # SQL_TXN_ISOLATION_OPTION
,    96 => 3                               # SQL_UNION
,    96 => 3                               # SQL_UNION_STATEMENT
,    47 => \&sql_user_name                 # SQL_USER_NAME
, 10000 => 1994                            # SQL_XOPEN_CLI_YEAR
);

1;
