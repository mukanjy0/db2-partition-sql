/*
 ==================
 |    employees   |
 ==================
 */
DROP TABLE IF EXISTS employees;
CREATE TABLE employees (
    emp_no int,
    birth_date date,
    first_name varchar(14),
    last_name varchar(16),
    gender character(1),
    hire_date date,
    dept_no varchar(5),
    from_date date
);

COPY employees (emp_no, birth_date, first_name, last_name, gender, hire_date, dept_no, from_date)
    FROM '/var/lib/postgresql/data/data2/employees.csv'
    DELIMITER ','
    CSV HEADER;

/*
 ==================
 |   employees2   |
 ==================
 */
-- DROP TABLE employees2;
CREATE TABLE employees2 (
    emp_no int,
    birth_date date,
    first_name varchar(14),
    last_name varchar(16),
    gender character(1),
    hire_date date,
    dept_no varchar(5),
    from_date date
)
PARTITION BY RANGE (DATE_PART('year', hire_date));

CREATE TABLE employees2_00 PARTITION OF employees2 FOR VALUES FROM (MINVALUE) TO (1988);
CREATE TABLE employees2_01 PARTITION OF employees2 FOR VALUES FROM (1988) TO (1994);
CREATE TABLE employees2_10 PARTITION OF employees2 FOR VALUES FROM (1994) TO (MAXVALUE);

COPY employees2 (emp_no, birth_date, first_name, last_name, gender, hire_date, dept_no, from_date)
    FROM '/var/lib/postgresql/data/data2/employees.csv'
    DELIMITER ','
    CSV HEADER;

/*
 ======================
 | queries (no index) |
 ======================
 */
SET enable_partition_pruning = ON;
VACUUM ANALYSE;

-- Query 1: from 1961 to 1987
EXPLAIN ANALYSE
SELECT * FROM employees WHERE date_part('year', hire_date) > 1960 AND date_part('year', hire_date) < 1988;
EXPLAIN ANALYSE
SELECT * FROM employees2 WHERE date_part('year', hire_date) > 1960 AND date_part('year', hire_date) < 1988;

-- Query 2: from 1989 to 1993
EXPLAIN ANALYSE
SELECT * FROM employees WHERE date_part('year', hire_date) > 1988 AND date_part('year', hire_date) < 1994;
EXPLAIN ANALYSE
SELECT * FROM employees2 WHERE date_part('year', hire_date) > 1988 AND date_part('year', hire_date) < 1994;
-- Query 3: from 1996 to present
EXPLAIN ANALYSE
SELECT * FROM employees WHERE date_part('year', hire_date) > 1995;
EXPLAIN ANALYSE
SELECT * FROM employees2 WHERE date_part('year', hire_date) > 1995;

/*
 ==================
 |   employees4   |
 ==================
 */
CREATE TABLE employees4 (
    emp_no int,
    birth_date date,
    first_name varchar(14),
    last_name varchar(16),
    gender character(1),
    hire_date date,
    dept_no varchar(5),
    from_date date
)
PARTITION BY RANGE (hire_date);

CREATE TABLE employees4_00 PARTITION OF employees4 FOR VALUES FROM (MINVALUE) TO ('1988-01-01');
CREATE TABLE employees4_01 PARTITION OF employees4 FOR VALUES FROM ('1988-01-01') TO ('1994-01-01');
CREATE TABLE employees4_10 PARTITION OF employees4 FOR VALUES FROM ('1994-01-01') TO (MAXVALUE);

COPY employees4 (emp_no, birth_date, first_name, last_name, gender, hire_date, dept_no, from_date)
    FROM '/var/lib/postgresql/data/data2/employees.csv'
    DELIMITER ','
    CSV HEADER;

/*
 =======================
 | queries (with index) |
 =======================
 */
CREATE INDEX IF NOT EXISTS employees_hire_year_idx ON employees USING btree (hire_date);
CREATE INDEX IF NOT EXISTS employees4_hire_year_idx ON employees4 USING btree (hire_date);

VACUUM ANALYSE;

-- Query 1
SET enable_seqscan = FALSE;
EXPLAIN ANALYSE
SELECT * FROM employees WHERE hire_date BETWEEN '1960-01-01' AND '1987-12-25';
EXPLAIN ANALYSE
SELECT * FROM employees4 WHERE hire_date BETWEEN '1960-01-01' AND '1987-12-25';

-- Query 2
EXPLAIN ANALYSE
SELECT * FROM employees WHERE hire_date BETWEEN '1989-02-15' AND '1993-02-15';
EXPLAIN ANALYSE
SELECT * FROM employees4 WHERE hire_date BETWEEN '1989-02-15' AND '1993-02-15';

-- Query 3
EXPLAIN ANALYSE
SELECT * FROM employees WHERE hire_date > '1994-07-28';
EXPLAIN ANALYSE
SELECT * FROM employees4 WHERE hire_date > '1994-07-28';