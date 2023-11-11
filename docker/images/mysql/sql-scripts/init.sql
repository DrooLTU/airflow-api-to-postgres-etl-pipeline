CREATE DATABASE IF NOT EXISTS loans;

CREATE ROLE IF NOT EXISTS 'loan_admin';

GRANT ALL PRIVILEGES ON loans.* TO 'loan_admin';

CREATE USER IF NOT EXISTS 'loan_user'@'%' IDENTIFIED BY 'userpassword';

GRANT SELECT, INSERT, UPDATE ON loans.* TO 'loan_user'@'%';

GRANT 'loan_user' TO 'loan_user'@'%';

FLUSH PRIVILEGES;