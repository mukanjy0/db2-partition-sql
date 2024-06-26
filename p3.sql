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
    from_date date,
    PRIMARY KEY (emp_no, dept_no)
);
CREATE INDEX employees_idx_emp_no ON employees USING hash(emp_no);

DROP TABLE IF EXISTS salaries;
CREATE TABLE salaries (
    emp_no int,
    salary int ,
    from_date date,
    to_date date,
    PRIMARY KEY (emp_no, from_date, to_date)
);
CREATE INDEX salaries_idx_emp_no ON salaries USING hash(emp_no);

COPY employees (emp_no, birth_date, first_name, last_name, gender, hire_date, dept_no, from_date)
    FROM '/var/lib/postgresql/data/data2/employees.csv'
    DELIMITER ','
    CSV HEADER;

COPY salaries (emp_no, salary, from_date, to_date)
    FROM '/var/lib/postgresql/data/data2/salaries.csv'
    DELIMITER ','
    CSV HEADER;

ALTER TABLE employees ADD COLUMN salary int;

UPDATE employees SET salary = s.salary
FROM salaries s
WHERE s.emp_no = employees.emp_no
AND s.to_date = (
    SELECT to_date
    FROM salaries
    WHERE emp_no = employees.emp_no
    ORDER BY to_date DESC
    LIMIT 1
);

-- Testing
SELECT * FROM employees LIMIT 10;

-- Write to csv
COPY employees TO '/var/lib/postgresql/data/data2/employees-with-salary.csv' DELIMITER ',' CSV HEADER;

/*
 ==================
 |   PK explore   |
 ==================
 */
-- Employees
select COUNT(*) from employees; -- 331 603
select COUNT(DISTINCT (emp_no, hire_date)) from employees; -- 300 024
select COUNT(DISTINCT (emp_no, hire_date, from_date)) from employees; -- 331 574
select COUNT(DISTINCT (emp_no, hire_date, dept_no)) from employees; -- 331 603
select COUNT(DISTINCT (emp_no, dept_no)) from employees; -- 331 603 good

-- Salaries
select COUNT(*) from salaries; -- 2 844 047
select COUNT(DISTINCT (emp_no, to_date)) from salaries; --2 843 891
select COUNT(DISTINCT (emp_no, salary)) from salaries; -- 2 843 453
select COUNT(DISTINCT (emp_no, from_date, to_date)) from salaries; -- 2 844 047 CANDIDATE KEY

/*
 ==================
 |   employees3   |
 ==================
 */
DROP TABLE IF EXISTS employees3;
CREATE TABLE employees3 (
    emp_no int,
    birth_date date,
    first_name varchar(14),
    last_name varchar(16),
    gender character(1),
    hire_date date,
    dept_no varchar(5),
    from_date date,
    salary int
)
PARTITION BY RANGE (hire_date, salary);

-- Partition vector for hire_date = [1988, 1994]
-- IMPORTANT : optimal partition vectors found in explore.ipynb

-- Subpartition vector for [MINVALUE, 1988] -> [66342, 80853]
CREATE TABLE employees3_h1_s1 PARTITION OF employees3 FOR VALUES FROM ('1985-01-01', MINVALUE) TO ('1988-01-01', 66342);
CREATE TABLE employees3_h1_s2 PARTITION OF employees3 FOR VALUES FROM ('1985-01-01', 66342) TO ('1988-01-01', 80853);
CREATE TABLE employees3_h1_s3 PARTITION OF employees3 FOR VALUES FROM ('1985-01-01', 80853) TO ('1988-01-01', MAXVALUE);
-- Subpartition vector for [1988, 1994] -> [60286, 74177]
CREATE TABLE employees3_h2_s1 PARTITION OF employees3 FOR VALUES FROM ('1988-01-01', MINVALUE) TO ('1994-01-01', 60286);
CREATE TABLE employees3_h2_s2 PARTITION OF employees3 FOR VALUES FROM ('1988-01-01', 60286) TO ('1994-01-01', 74177);
CREATE TABLE employees3_h2_s3 PARTITION OF employees3 FOR VALUES FROM ('1988-01-01', 74177) TO ('1994-01-01', MAXVALUE);
-- Subpartition vector for [1994, MAXVALUE] -> [52667, 66268]
CREATE TABLE employees3_h3_s1 PARTITION OF employees3 FOR VALUES FROM ('1994-01-01', MINVALUE) TO ('2000-01-29', 52667);
CREATE TABLE employees3_h3_s2 PARTITION OF employees3 FOR VALUES FROM ('1994-01-01', 52667) TO ('2000-01-29', 66268);
CREATE TABLE employees3_h3_s3 PARTITION OF employees3 FOR VALUES FROM ('1994-01-01', 66268) TO ('2000-01-29', MAXVALUE);
