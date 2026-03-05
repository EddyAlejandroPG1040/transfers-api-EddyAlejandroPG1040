package repositories

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"transfers-api/internal/config"
	"transfers-api/internal/enums"
	"transfers-api/internal/known_errors"
	"transfers-api/internal/logging"
	"transfers-api/internal/models"

	_ "github.com/go-sql-driver/mysql"
)

type TransfersMySQLRepo struct {
	db *sql.DB
}

type transferMySQLDAO struct {
	ID         string
	SenderID   string
	ReceiverID string
	Currency   string
	Amount     float64
	State      string
}

func NewTransfersMySQLRepository(cfg config.MySQL) *TransfersMySQLRepo {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		cfg.Username, cfg.Password, cfg.Hostname, cfg.Port, cfg.Database,
	)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		logging.Logger.Fatalf("error connecting to MySQL: %v", err)
	}

	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)

	if err := db.Ping(); err != nil {
		logging.Logger.Fatalf("error pinging MySQL: %v", err)
	}

	return &TransfersMySQLRepo{db: db}
}

func (r *TransfersMySQLRepo) Create(ctx context.Context, transfer models.Transfer) (string, error) {
	query := `
		INSERT INTO transfers (sender_id, receiver_id, currency, amount, state)
		VALUES (?, ?, ?, ?, ?)
	`

	res, err := r.db.ExecContext(ctx, query,
		transfer.SenderID,
		transfer.ReceiverID,
		transfer.Currency.String(),
		transfer.Amount,
		transfer.State,
	)
	if err != nil {
		return "", fmt.Errorf("error inserting transfer in MySQL: %w", err)
	}

	insertedID, err := res.LastInsertId()
	if err != nil {
		return "", fmt.Errorf("error getting last inserted ID: %w", err)
	}

	return fmt.Sprintf("%d", insertedID), nil
}

func (r *TransfersMySQLRepo) GetByID(ctx context.Context, id string) (models.Transfer, error) {
	query := `
		SELECT id, sender_id, receiver_id, currency, amount, state
		FROM transfers
		WHERE id = ?
	`

	var dao transferMySQLDAO
	err := r.db.QueryRowContext(ctx, query, id).Scan(
		&dao.ID,
		&dao.SenderID,
		&dao.ReceiverID,
		&dao.Currency,
		&dao.Amount,
		&dao.State,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return models.Transfer{}, fmt.Errorf("transfer not found: %w", known_errors.ErrNotFound)
		}
		return models.Transfer{}, fmt.Errorf("error getting transfer: %w", err)
	}

	return models.Transfer{
		ID:         dao.ID,
		SenderID:   dao.SenderID,
		ReceiverID: dao.ReceiverID,
		Currency:   enums.ParseCurrency(dao.Currency),
		Amount:     dao.Amount,
		State:      dao.State,
	}, nil
}

func (r *TransfersMySQLRepo) Update(ctx context.Context, transfer models.Transfer) error {
	query := `
		UPDATE transfers
		SET sender_id   = COALESCE(NULLIF(?, ''), sender_id),
		    receiver_id = COALESCE(NULLIF(?, ''), receiver_id),
		    currency    = COALESCE(NULLIF(?, ''), currency),
		    amount      = COALESCE(NULLIF(?, 0),  amount),
		    state       = COALESCE(NULLIF(?, ''), state)
		WHERE id = ?
	`

	res, err := r.db.ExecContext(ctx, query,
		transfer.SenderID,
		transfer.ReceiverID,
		transfer.Currency.String(),
		transfer.Amount,
		transfer.State,
		transfer.ID,
	)
	if err != nil {
		return fmt.Errorf("error updating transfer: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("error checking rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("transfer not found: %w", known_errors.ErrNotFound)
	}

	return nil
}

func (r *TransfersMySQLRepo) Delete(ctx context.Context, id string) error {
	query := `DELETE FROM transfers WHERE id = ?`

	res, err := r.db.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("error deleting transfer: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("error checking rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("transfer not found: %w", known_errors.ErrNotFound)
	}

	return nil
}
