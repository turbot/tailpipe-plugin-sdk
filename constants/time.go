package constants

import "time"

// DefaultInitialCollectionPeriod defines the default initial collection period for a row source.
// (collect for 7 days for first collection)
const DefaultInitialCollectionPeriod = 7 * 24 * time.Hour
