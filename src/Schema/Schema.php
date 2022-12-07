<?php

namespace Brokenice\LaravelMysqlPartition\Schema;

use Brokenice\LaravelMysqlPartition\Exceptions\UnexpectedValueException;
use Brokenice\LaravelMysqlPartition\Exceptions\UnsupportedPartitionException;
use Brokenice\LaravelMysqlPartition\Models\Partition;
use Carbon\Carbon;
use Carbon\CarbonInterface;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema as IlluminateSchema;

/**
 * Class PartitionHelper method.
 */
class Schema extends IlluminateSchema
{

    public static $have_partitioning = false;
    public static $already_checked = false;

    // Array of months
    static protected $month = [
        12 => 'dec',
        1 => 'jan',
        2 => 'feb',
        3 => 'mar',
        4 => 'apr',
        5 => 'may',
        6 => 'jun',
        7 => 'jul',
        8 => 'aug',
        9 => 'sep',
        10 => 'oct',
        11 => 'nov'
    ];

    public static function isPostgres(){
        $driver = config(sprintf('database.connections.%s.driver', DB::getDefaultConnection()));
        return $driver === 'pgsql';
    }

    /**
     * returns array of partition names for a specific db/table
     *
     * @param string $db    database name
     * @param string $table table name
     *
     * @access  public
     * @return array of partition names
     */
    public static function getPartitionNames($db, $table)
    {
        self::assertSupport();
        return DB::select(DB::raw(
            "SELECT `PARTITION_NAME`, `SUBPARTITION_NAME`, `PARTITION_ORDINAL_POSITION`, `TABLE_ROWS`, `PARTITION_METHOD` FROM `information_schema`.`PARTITIONS`"
            . " WHERE `TABLE_SCHEMA` = '" . $db
            . "' AND `TABLE_NAME` = '" . $table . "'"
        ));
    }

    /**
     * checks if MySQL server supports partitioning
     *
     * @static
     * @staticvar boolean $have_partitioning
     * @staticvar boolean $already_checked
     * @access  public
     * @return boolean
     */
    public static function havePartitioning()
    {
        if (self::isPostgres()){
            self::$already_checked = true;
            self::$have_partitioning = true;
            return self::$have_partitioning;
        }
        if (self::$already_checked) {
            return self::$have_partitioning;
        }
        
        if (version_compare(self::version(), 8, '>=')) {
            self::$have_partitioning = true;
        }

        elseif (version_compare(self::version(), 5.6, '>=') && version_compare(self::version(), 8, '<')) {
            // see http://dev.mysql.com/doc/refman/5.6/en/partitioning.html
            $plugins = DB::connection()->getPdo()->query("SHOW PLUGINS")->fetchAll();
            foreach ($plugins as $value) {
                if ($value['Name'] === 'partition') {
                    self::$have_partitioning = true;
                    break;
                }
            }
        }
        elseif (version_compare(self::version(), 5.1, '>=') && version_compare(self::version(), 5.6, '<')) {
            if (DB::connection()->getPdo()->query("SHOW VARIABLES LIKE 'have_partitioning';")->fetchAll()) {
                self::$have_partitioning = true;
            }
        }
        else {
            self::$have_partitioning = false;
        }

        self::$already_checked = true;
        return self::$have_partitioning;
    }

    /**
     * Implode array of partitions with comma
     * @param $partitions
     * @return string
     */
    private static function implodePartitions($partitions)
    {
        return collect($partitions)->map(static function($partition){
            return $partition->toSQL();
        })->implode(',');
    }

    /**
     * Creates a partitioned table, ONLY for Postgresql.
     *
     * @param string $table The table name to create.
     * @param string $column The column that is going to be used for partitioning.
     * @param string $type The type of column that is used for partitioning. e.g. timestamp
     * @param string $partitionType The partitioning type. HASH, LIST or RANGE.
     * @param bool $nullable Whether the column has to be nullable or not.
     * @return void
     */
    public static function createPartitionedTable($table, $column, $type, $partitionType = 'RANGE', $nullable = false){
        if (!self::isPostgres()){
            return;
        }
        $nullableStatement = '';
        if ($nullable === false){
            $nullableStatement = ' NOT NULL ';
        }

        DB::unprepared(
            sprintf(
                'CREATE TABLE %s (%s %s%s) PARTITION BY %s (%s);',
                $table,
                $column,
                $type,
                $nullableStatement,
                $partitionType,
                $column
            )
        );
    }

    /**
     * @param $table
     * @param $column
     * @param null $schema
     */
    public static function partitionByMonths($table, $column, $schema=null)
    {
        $appendSchema = $schema !== null ? ($schema.".") : '';
        // Build query
        $query = "ALTER TABLE {$appendSchema}{$table} PARTITION BY RANGE(MONTH({$column})) ( ";
        $query .= "PARTITION `jan` VALUES LESS THAN (2),";
        $query .= "PARTITION `feb` VALUES LESS THAN (3),";
        $query .= "PARTITION `mar` VALUES LESS THAN (4),";
        $query .= "PARTITION `apr` VALUES LESS THAN (5),";
        $query .= "PARTITION `may` VALUES LESS THAN (6),";
        $query .= "PARTITION `jun` VALUES LESS THAN (7),";
        $query .= "PARTITION `jul` VALUES LESS THAN (8),";
        $query .= "PARTITION `aug` VALUES LESS THAN (9),";
        $query .= "PARTITION `sep` VALUES LESS THAN (10),";
        $query .= "PARTITION `oct` VALUES LESS THAN (11),";
        $query .= "PARTITION `nov` VALUES LESS THAN (12),";
        $query .= "PARTITION `dec` VALUES LESS THAN (13)";
        $query .= ")";
        DB::unprepared(DB::raw($query));
    }

    /**
     * Partition table by years and months, Supports Postgresql.
     *
     * @param $table
     * @param $column
     * @param $startYear
     * @param null $endYear
     * @param bool $includeFuturePartition
     * @param null $schema
     * @param bool $timestamp Whether the column is of type TIMESTAMP or not.
     */
    public static function partitionByYearsAndMonths($table, $column, $startYear, $endYear = null, $includeFuturePartition = true, $schema=null, $timestamp = false)
    {
        $appendSchema = $schema !== null ? ($schema.".") : '';
        $isPostgres = self::isPostgres();
        self::assertSupport();
        $endYear = $endYear ?: date('Y');
        if ($startYear > $endYear){
            throw new UnexpectedValueException("$startYear must be lower than $endYear");
        }

        if ($isPostgres){
            foreach (range($startYear, $endYear) as $year) {
                for ($monthCounter = 1; $monthCounter < 13; $monthCounter++) {
                    $partitionName = sprintf('%s_%s%d', $table, self::$month[$monthCounter], $year);
                    $dateObj = Carbon::create($year, $monthCounter, 1);
                    $dateObjMonthAdded = Carbon::create($year, $monthCounter, 1)->addMonth();
                    $query = sprintf("CREATE TABLE %s PARTITION OF %s FOR VALUES FROM ('%04d-%02d-%02d') TO ('%04d-%02d-%02d');", $partitionName, $table, $dateObj->year, $dateObj->month, $dateObj->day, $dateObjMonthAdded->year, $dateObjMonthAdded->month, $dateObjMonthAdded->day);
                    DB::unprepared(DB::raw($query));
                }
            }
            return;
        }
        // Build partitions array for years range
        $partitions = [];
        foreach (range($startYear, $endYear) as $year) {
            $partitions[] = new Partition('year'.$year, Partition::RANGE_TYPE, $year+1);
        }
        // Build query

        $rangeSelector = $timestamp === false ? "YEAR($column)" : "UNIX_TIMESTAMP($column)";
        $query = "ALTER TABLE {$appendSchema}{$table} PARTITION BY RANGE($rangeSelector) SUBPARTITION BY HASH(MONTH({$column})) ( ";
        $subPartitionsQuery = collect($partitions)->map(static function($partition) {
            return $partition->toSQL() . "(". collect(self::$month)->map(static function($month) use ($partition){
                    return "SUBPARTITION {$month}".($partition->value-1);
                })->implode(', ') . ' )';
        });
        $query .= collect($subPartitionsQuery)->implode(',');
        // Include future partitions if needed
        if($includeFuturePartition) {
            $query .= ", PARTITION future VALUES LESS THAN (MAXVALUE) (";
            $query .= collect(self::$month)->map(static function ($month) {
                return "SUBPARTITION `{$month}`";
            })->implode(', ');
            $query .= ") )";
        } else {
            $query .= ")";
        }
        DB::unprepared(DB::raw($query));
    }

    /**
     * Partition table by range
     * # WARNING 1: A PRIMARY KEY must include all columns in the table's partitioning function
     * @param $table
     * @param $column
     * @param Partition[] $partitions
     * @param bool $includeFuturePartition
     * @param null $schema
     * @static public
     */
    public static function partitionByRange($table, $column, $partitions, $includeFuturePartition = true, $schema=null)
    {
        $appendSchema = $schema !== null ? ($schema.".") : '';
        self::assertSupport();
        $query = "ALTER TABLE {$appendSchema}{$table} PARTITION BY RANGE({$column}) (";
        $query .= self::implodePartitions($partitions);
        if($includeFuturePartition){
            $query .= ", PARTITION future VALUES LESS THAN (MAXVALUE)";
        }
        $query = trim(trim($query),',') . ')';
        DB::unprepared(DB::raw($query));

    }

    /**
     * Partition table by year, Supports Postgresql.
     * @param $table
     * @param $column
     * @param $startYear
     * @param $endYear
     * @param null $schema
     * @param bool $timestamp Whether the column is of type TIMESTAMP or not.
     */
    public static function partitionByYears($table, $column, $startYear, $endYear = null, $schema = null, $timestamp = false)
    {
        $appendSchema = $schema !== null ? ($schema.".") : '';
        $endYear = $endYear ?: date('Y');
        if ($startYear > $endYear){
            throw new UnexpectedValueException("$startYear must be lower than $endYear");
        }
        if (self::isPostgres()){
            foreach (range($startYear, $endYear) as $year) {
                $partitionName = sprintf('%s_%d', $table, $year);
                $dateObj = Carbon::create($year);
                $dateObjYearAdded = Carbon::create($year)->addYear();
                $query = sprintf("CREATE TABLE %s PARTITION OF %s FOR VALUES FROM ('%04d-%02d-%02d') TO ('%04d-%02d-%02d');", $partitionName, $table, $dateObj->year, $dateObj->month, $dateObj->day, $dateObjYearAdded->year, $dateObjYearAdded->month, $dateObjYearAdded->day);
                DB::unprepared(DB::raw($query));
            }
            return;
        }
        $partitions = [];
        foreach (range($startYear, $endYear) as $year) {
            $partitions[] = new Partition('year'.$year, Partition::RANGE_TYPE, $timestamp === false ? $year+1 : sprintf("UNIX_TIMESTAMP('%s')", sprintf('%s-01-01 00:00:00', $year + 1)));
        }
        self::partitionByRange($table, $timestamp === false ? "YEAR($column)" : "UNIX_TIMESTAMP($column)", $partitions, true, $schema);
    }

    /**
     * Partition table by list
     * # WARNING 1: A PRIMARY KEY must include all columns in the table's partitioning function
     * @param $table
     * @param $column
     * @param Partition[] $partitions
     * @param null $schema
     * @static public
     *
     */
    public static function partitionByList($table, $column, $partitions, $schema=null)
    {
        $appendSchema = $schema !== null ? ($schema.".") : '';
        self::assertSupport();
        $query = "ALTER TABLE {$appendSchema}{$table} PARTITION BY LIST({$column}) (";
        $query .= self::implodePartitions($partitions);
        $query .= ')';
        DB::unprepared(DB::raw($query));
    }

    /**
     * Partition table by hash
     * # WARNING 1: A PRIMARY KEY must include all columns in the table's partitioning function
     * @param $table
     * @param $hashColumn
     * @param $partitionsNumber
     * @param null $schema
     * @static public
     */
    public static function partitionByHash($table, $hashColumn, $partitionsNumber, $schema=null)
    {
        $appendSchema = $schema !== null ? ($schema.".") : '';
        self::assertSupport();
        $query = "ALTER TABLE {$appendSchema}{$table} PARTITION BY HASH({$hashColumn}) ";
        $query .= "PARTITIONS {$partitionsNumber};";
        DB::unprepared(DB::raw($query));
    }

    /**
     * Partition table by hash
     * # WARNING 1: Are used all primary and unique keys
     * @param $table
     * @param $partitionsNumber
     * @param null $schema
     * @static public
     */
    public static function partitionByKey($table, $partitionsNumber, $schema=null)
    {
            $appendSchema = $schema !== null ? ($schema.".") : '';
            self::assertSupport();
            $query = "ALTER TABLE {$appendSchema}{$table} PARTITION BY KEY() ";
            $query .= "PARTITIONS {$partitionsNumber};";
            DB::unprepared(DB::raw($query));
        }

    /**
     * Check mysql version
     *
     * @static public
     * @return string
     */
    public static function version()
    {
        $pdo = DB::connection()->getPdo();
        return $pdo->query('select version()')->fetchColumn();
    }

    /**
     * Force field to be autoIncrement, Supports Postgresql.
     * @param $table
     * @param string $field
     * @param string $type
     */
    public static function forceAutoIncrement($table, $field = 'id', $type='INTEGER')
    {
        if (self::isPostgres()){
            DB::statement(sprintf('CREATE SEQUENCE IF NOT EXISTS %s_%s_seq;', $table, $field));
            DB::statement(sprintf("SELECT SETVAL('%s_%s_seq', (SELECT max(%s) FROM %s));", $table, $field, $field, $table));
            DB::statement(sprintf("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT nextval('%s_%s_seq'::regclass);", $table, $field, $table, $field));
            DB::statement(sprintf("ALTER SEQUENCE %s_%s_seq OWNED BY %s.%s;", $table, $field, $table, $field));
            return;
        }
        DB::statement("ALTER TABLE {$table} MODIFY {$field} {$type} NOT NULL AUTO_INCREMENT");
    }

    /**
     * Delete the rows of a partition without affecting the rest of the dataset in the table
     * @param $table
     * @param $partitions
     */
    public static function truncatePartitionData($table, $partitions)
    {
        DB::statement("ALTER TABLE {$table} TRUNCATE PARTITION " . implode(', ', $partitions));
    }

    /**
     * Delete the rows of a partition without affecting the rest of the dataset in the table
     * @param $table
     * @param $partitions
     */
    public static function deletePartition($table, $partitions)
    {
        DB::statement("ALTER TABLE {$table} DROP PARTITION " . implode(', ', $partitions));
    }

    /**
     * Rebuilds the partition; this has the same effect as dropping all records stored in the partition,
     * then reinserting them. This can be useful for purposes of defragmentation.
     * @param $table
     * @param string[] $partitions
     */
    public static function rebuildPartitions($table, $partitions)
    {
        DB::statement("ALTER TABLE {$table} REBUILD PARTITION " . implode(', ', $partitions));
    }

    /**
     * If you have deleted a large number of rows from a partition or if you have made many changes to a partitioned table
     * with variable-length rows (that is, having VARCHAR, BLOB, or TEXT columns), you can use this method
     * to reclaim any unused space and to defragment the partition data file.
     * @param $table
     * @param string[] $partitions
     * @return array
     */
    public static function optimizePartitions($table, $partitions)
    {
        return DB::select(DB::raw("ALTER TABLE {$table} OPTIMIZE PARTITION " . implode(', ', $partitions)));
    }

    /**
     * This reads and stores the key distributions for partitions.
     * @param $table
     * @param string[] $partitions
     * @return array
     */
    public static function analyzePartitions($table, $partitions)
    {
        return DB::select(DB::raw("ALTER TABLE {$table} ANALYZE PARTITION " . implode(', ', $partitions)));
    }

    /**
     * Normally, REPAIR PARTITION fails when the partition contains duplicate key errors. In MySQL 5.7.2 and later,
     * you can use ALTER IGNORE TABLE with this option, in which case all rows that cannot be moved due to the presence
     * of duplicate keys are removed from the partition (Bug #16900947).
     * @param $table
     * @param string[] $partitions
     * @return array
     */
    public static function repairPartitions($table, $partitions)
    {
        return DB::select(DB::raw("ALTER TABLE {$table} REPAIR PARTITION " . implode(', ', $partitions)));
    }

    /**
     * You can check partitions for errors in much the same way that you can use CHECK TABLE with non partitioned tables.
     * @param $table
     * @param string[] $partitions
     * @return array
     */
    public static function checkPartitions($table, $partitions)
    {
        return DB::select(DB::raw("ALTER TABLE {$table} CHECK PARTITION " . implode(', ', $partitions)));
    }

    /**
     * todo: Reorganize partition
     * ALTER TABLE mytable REORGANIZE PARTITION future INTO ( PARTITION yearCurrent VALUES LESS THAN (yearCurrent+1), PARTITION future VALUES LESS THAN MAXVALUE);
     */

    /**
     * Assert support for partition
     * @throws UnsupportedPartitionException
     */
    private static function assertSupport()
    {
        if (!self::havePartitioning()) {
            throw new UnsupportedPartitionException('Partitioning is unsupported on your server version');
        }
    }
}
