package main

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

type Storage interface {
	CreateAccount(*Account) error
	DeleteAccount(int) error
	UpdateAccount(*Account) error
	GetAccounts() ([]*Account, error)
	GetAccountByID(int) (*Account, error)
	GetAccountByNumber(int) (*Account, error)
}

type PostgresStore struct {
	db *sql.DB
}

func NewPostgresStore() (*PostgresStore, error) {
	connStr := "user=postgres dbname=postgres password=postgres sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return &PostgresStore{
		db: db,
	}, nil
}

func (s *PostgresStore) Init() error {
	return s.initTables()
}

func (s *PostgresStore) initTables() error {
	query := `
	CREATE TABLE IF NOT EXISTS cust (
    cnum INTEGER NOT NULL,
    cname VARCHAR(10) NOT NULL,
    city VARCHAR(10) NOT NULL,
    rating INTEGER NOT NULL,
    snum INTEGER,
    PRIMARY KEY (cnum)
	);


	CREATE TABLE IF NOT EXISTS SAL (
    snum INTEGER NOT NULL,
    sname VARCHAR(10) NOT NULL,
    city VARCHAR(10) NOT NULL,
    comm NUMERIC(7,2) NOT NULL,
    PRIMARY KEY (snum)
	);

	CREATE TABLE IF NOT EXISTS ord (
    onum INTEGER NOT NULL,
    amt NUMERIC(7,2) NOT NULL,
    odate DATE NOT NULL,
    cnum INTEGER,
    snum INTEGER,
    PRIMARY KEY (onum)
	);

	INSERT INTO sal (snum, sname, city, comm) VALUES (1001, 'Peel', 'London',
	0.12);
	INSERT INTO sal (snum, sname, city, comm) VALUES (1002, 'Serres', 'San
	Jose', 0.13);
	INSERT INTO sal (snum, sname, city, comm) VALUES (1004, 'Motica',
	'London', 0.11);
	INSERT INTO sal (snum, sname, city, comm) VALUES (1007, 'Rifkin',
	'Barcelona', 0.15);
	INSERT INTO sal (snum, sname, city, comm) VALUES (1003, 'Axelrod', 'New
	York', 0.10);

	INSERT INTO cust (cnum, cname, city, rating, snum) VALUES (2001,
	'Hoffman', 'London', 100, 1001);
	INSERT INTO cust (cnum, cname, city, rating, snum) VALUES (2002,
	'Giovanni', 'Rome', 200, 1003);
	INSERT INTO cust (cnum, cname, city, rating, snum) VALUES (2003, 'Liu',
	'San Jose', 200, 1002);
	INSERT INTO cust (cnum, cname, city, rating, snum) VALUES (2004, 'Grass',
	'Berlin', 300, 1002);
	INSERT INTO cust (cnum, cname, city, rating, snum) VALUES (2006,
	'Clemens', 'London', 100, 1001);
	INSERT INTO cust (cnum, cname, city, rating, snum) VALUES (2008, 'Cisneros',
	'San Jose', 300, 1007);
	INSERT INTO cust (cnum, cname, city, rating, snum) VALUES (2007, 'Pereira',
	'Rome', 100, 1004);


	INSERT INTO ord VALUES (3001, 18.69, '2022-08-03', 2008, 1007);
	INSERT INTO ord VALUES (3003, 767.19, '2022-08-03', 2001, 1001);
	INSERT INTO ord VALUES (3002, 1900.10, '2022-08-03', 2007, 1004);
	INSERT INTO ord VALUES (3005, 5160.45, '2022-08-03', 2003, 1002);
	INSERT INTO ord VALUES (3006, 1098.16, '2022-08-03', 2008, 1007);
	INSERT INTO ord VALUES (3009, 1713.23, '2022-08-04', 2002, 1003);
	INSERT INTO ord VALUES (3007, 75.75, '2022-08-04', 2004, 1002);
	INSERT INTO ord VALUES (3008, 4723.00, '2022-08-05', 2006, 1001);
	INSERT INTO ord VALUES (3010, 1309.95, '2022-08-06', 2004, 1002);
	INSERT INTO ord VALUES (3011, 9891.88, '2022-08-06', 2006, 1001);
	`
	_, err := s.db.Exec(query)
	return err
}

func (s *PostgresStore) CreateAccount(acc *Account) error {
	query := `insert into account 
	(first_name, last_name, number, encrypted_password, balance, created_at)
	values ($1, $2, $3, $4, $5, $6)`

	_, err := s.db.Query(
		query,
		acc.FirstName,
		acc.LastName,
		acc.Number,
		acc.EncryptedPassword,
		acc.Balance,
		acc.CreatedAt)

	if err != nil {
		return err
	}

	return nil
}

func (s *PostgresStore) UpdateAccount(*Account) error {
	return nil
}

func (s *PostgresStore) DeleteAccount(id int) error {
	_, err := s.db.Query("delete from account where id = $1", id)
	return err
}

func (s *PostgresStore) GetAccountByNumber(number int) (*Account, error) {
	rows, err := s.db.Query("select * from account where number = $1", number)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		return scanIntoAccount(rows)
	}

	return nil, fmt.Errorf("account with number [%d] not found", number)
}

func (s *PostgresStore) GetAccountByID(id int) (*Account, error) {
	rows, err := s.db.Query("select * from account where id = $1", id)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		return scanIntoAccount(rows)
	}

	return nil, fmt.Errorf("account %d not found", id)
}

func (s *PostgresStore) GetAccounts() ([]*Account, error) {
	rows, err := s.db.Query("select * from account")
	if err != nil {
		return nil, err
	}

	accounts := []*Account{}
	for rows.Next() {
		account, err := scanIntoAccount(rows)
		if err != nil {
			return nil, err
		}
		accounts = append(accounts, account)
	}

	return accounts, nil
}

func scanIntoAccount(rows *sql.Rows) (*Account, error) {
	account := new(Account)
	err := rows.Scan(
		&account.ID,
		&account.FirstName,
		&account.LastName,
		&account.Number,
		&account.EncryptedPassword,
		&account.Balance,
		&account.CreatedAt)

	return account, err
}
