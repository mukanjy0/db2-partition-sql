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
 |   employees1   |
 ==================
 */
DROP TABLE IF EXISTS employees1;
CREATE TABLE employees1 (
    emp_no int,
    birth_date date,
    first_name varchar(14),
    last_name varchar(16),
    gender character(1),
    hire_date date,
    dept_no varchar(5),
    from_date date
)
PARTITION BY LIST (dept_no);

CREATE TABLE employees1_d004 PARTITION OF employees1 FOR VALUES IN ('d004');
CREATE TABLE employees1_d005 PARTITION OF employees1 FOR VALUES IN ('d005');
CREATE TABLE employees1_d007 PARTITION OF employees1 FOR VALUES IN ('d007');
CREATE TABLE employees1_default PARTITION OF employees1 DEFAULT;

COPY employees1
FROM '/var/lib/postgresql/data/data2/employees.csv'
DELIMITER ','
CSV HEADER;

/*
 ==================
 |   queries   |
 ==================
 */
SET enable_partition_pruning = ON;
VACUUM ANALYSE;

-- dept 50
EXPLAIN ANALYSE
SELECT * FROM employees WHERE dept_no = 'd004';

EXPLAIN ANALYSE
SELECT * FROM employees1 WHERE dept_no = 'd004';

-- dept 80
EXPLAIN ANALYSE
SELECT * FROM employees WHERE dept_no = 'd005';

EXPLAIN ANALYSE
SELECT * FROM employees1 WHERE dept_no = 'd005';

-- dept 100
EXPLAIN ANALYSE
SELECT * FROM employees WHERE dept_no = 'd007';

EXPLAIN ANALYSE
SELECT * FROM employees1 WHERE dept_no = 'd007';
