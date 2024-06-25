/*
 ==================
 |    employees   |
 ==================
 */
DROP TABLE employees;
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


CREATE TABLE salaries (
                          emp_no int,
                          salary int ,
                          from_date date,
                          to_date date
);

COPY employees (emp_no, birth_date, first_name, last_name, gender, hire_date, dept_no, from_date)
    FROM '/var/lib/postgresql/data/data2/employees.csv'
    DELIMITER ','
    CSV HEADER;

COPY salaries (emp_no, salary, from_date, to_date)
    FROM '/var/lib/postgresql/data/data2/salaries.csv'
    DELIMITER ','
    CSV HEADER;

ALTER TABLE employees ADD COLUMN salary int;
UPDATE employees SET salary = (
    SELECT salary
    FROM salaries
    WHERE emp_no = employees.emp_no
    ORDER BY to_date DESC
    LIMIT 1
);

/*
 ==================
 |   employees3   |
 ==================
 */
-- DROP TABLE employees3;
CREATE TABLE employees3 (
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

