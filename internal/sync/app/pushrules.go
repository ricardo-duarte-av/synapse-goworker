package app

import (
	"context"
	"encoding/json"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/pushrules"
)

// PushRulesContent is push_rules_for_user: the m.push_rules content sync
// reports, for GET .../account_data/m.push_rules on the account-data
// writer.
func (s *Sync) PushRulesContent(ctx context.Context, userID string) (json.RawMessage, error) {
	rules, enabled, err := s.db.PushRules(ctx, userID)
	if err != nil {
		return nil, err
	}
	return pushrules.Format(userID, rules, enabled, s.pushFeatures)
}

// PushRuleFeatures are the flags the push rules are built under.
func (s *Sync) PushRuleFeatures() pushrules.Features { return s.pushFeatures }
