package main

import (
	"encoding/csv"
	"os"
	"strconv"
	"strings"
	"time"
)

type POSTransaction struct {
	StoreID   string
	TxnID     string
	Timestamp time.Time
	AmountINR float64
}

func loadPOSTransactions(path string) ([]POSTransaction, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	reader := csv.NewReader(f)
	rows, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	var txns []POSTransaction
	for i, row := range rows {
		if i == 0 || len(row) < 4 {
			continue
		}
		ts, err := time.Parse(time.RFC3339, strings.TrimSpace(row[2]))
		if err != nil {
			continue
		}
		amount, _ := strconv.ParseFloat(strings.TrimSpace(row[3]), 64)
		txns = append(txns, POSTransaction{
			StoreID:   strings.TrimSpace(row[0]),
			TxnID:     strings.TrimSpace(row[1]),
			Timestamp: ts,
			AmountINR: amount,
		})
	}
	return txns, nil
}

// correlatePOS marks visitors as converted when a POS txn falls within 5 minutes
// after billing-zone activity for the same store.
func correlatePOS(tracker *MetricTracker, storeID string, txns []POSTransaction) int {
	converted := 0
	for _, txn := range txns {
		if txn.StoreID != storeID {
			continue
		}
		for visitorID, sess := range tracker.Sessions {
			if sess.Converted || !sess.JoinedBilling {
				continue
			}
			if !sess.LastActivity.IsZero() && txn.Timestamp.Sub(sess.LastActivity) <= 5*time.Minute && txn.Timestamp.After(sess.LastActivity) {
				tracker.MarkConverted(visitorID)
				converted++
			}
		}
	}
	return converted
}
