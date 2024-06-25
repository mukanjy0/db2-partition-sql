/*
 ==================
 |    employees   |
 ==================
 */
CREATE TABLE employees
( employee_id    INTEGER
    , first_name     VARCHAR(20)
    , last_name      VARCHAR(25) NOT NULL
    , email          VARCHAR(25) NOT NULL
    , phone_number   VARCHAR(20)
    , hire_date      TIMESTAMP  NOT NULL
    , job_id         VARCHAR(10) NOT NULL
    , salary         NUMERIC(8,2)
    , commission_pct NUMERIC(2,2)
    , manager_id     INTEGER
    , department_id  INTEGER
    , CONSTRAINT     emp_salary_min  CHECK (salary > 0)
);

COPY employees (employee_id, first_name, last_name, email, phone_number, hire_date, job_id, salary, commission_pct, manager_id, department_id)
    FROM '/var/lib/postgresql/data/data1/employees.csv'
    DELIMITER ';'
    CSV HEADER;

/*
 ==================
 |   employees1   |
 ==================
 */
-- DROP TABLE employees1;
CREATE TABLE employees1 (
    employee_id INT NOT NULL,
    first_name VARCHAR(11),
    last_name VARCHAR(11),
    email VARCHAR(255),
    phone_number VARCHAR(18),
    hire_date TIMESTAMP,
    job_id VARCHAR(10),
    salary NUMERIC(8,2),
    commission_pct NUMERIC(2,2),
    manager_id INT,
    department_id iNT
)
PARTITION BY LIST (department_id);

CREATE TABLE employess1_dpt50 PARTITION OF employees1 FOR VALUES IN (50);
CREATE TABLE employess1_dpt80 PARTITION OF employees1 FOR VALUES IN (80);
CREATE TABLE employess1_dpt100 PARTITION OF employees1 FOR VALUES IN (100);

COPY employees1 (employee_id, first_name, last_name, email, phone_number, hire_date, job_id, salary, commission_pct, manager_id, department_id)
FROM '/var/lib/postgresql/data/data1/employees.csv'
DELIMITER ';'
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
SELECT * FROM employees WHERE department_id = 50;

EXPLAIN ANALYSE
SELECT * FROM employees1 WHERE department_id = 50;

-- dept 80
EXPLAIN ANALYSE
SELECT * FROM employees WHERE department_id = 80;

EXPLAIN ANALYSE
SELECT * FROM employees1 WHERE department_id = 80;

-- dept 100
EXPLAIN ANALYSE
SELECT * FROM employees WHERE department_id = 100;

EXPLAIN ANALYSE
SELECT * FROM employees1 WHERE department_id = 100;
