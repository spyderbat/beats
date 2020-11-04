// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package auditd

import (
	"fmt"
	"os"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	"github.com/elastic/beats/v7/auditbeat/cmd"
	"github.com/elastic/go-libaudit/v2"
)

func init() {
	deleteRules := cobra.Command{
		Use:     "auditd-rules-delete",
		Short:   "Delete currently installed auditd rules",
		Aliases: []string{"audit-rules-delete", "audit_rules_delete", "rules_delete", "auditdrulesdelete", "auditrulesdelete"},
		Run: func(cmd *cobra.Command, args []string) {
			if err := deleteAuditdRules(); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to delete auditd rules: %v\n", err)
				os.Exit(1)
			}
		},
	}

	cmd.DeleteCmd.AddCommand(&deleteRules)
}

func deleteAuditdRules() error {
	client, err := libaudit.NewAuditClient(nil)
	if err != nil {
		return errors.Wrap(err, "failed to create audit client")
	}
	defer client.Close()

	// Don't attempt to change configuration if audit rules are locked (enabled == 2).
	// Will result in EPERM.
	status, err := client.GetStatus()
	if err != nil {
		err = errors.Wrap(err, "failed to get audit status before adding rules")
		return err
	}
	if status.Enabled == auditLocked {
		return errors.New("Skipping rule configuration: Audit rules are locked")
	}

	// Delete existing rules.
	_, err = client.DeleteRules()
	if err != nil {
		return errors.Wrap(err, "failed to delete existing rules")
	}

	return nil
}
