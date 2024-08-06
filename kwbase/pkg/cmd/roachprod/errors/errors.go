// Copyright 2020 The Cockroach Authors.
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package errors

import (
	"fmt"
	"os/exec"

	"github.com/cockroachdb/errors"
)

// Error is an interface for error types used by the main.wrap() function
// to output correctly classified log messages and exit codes.
type Error interface {
	error

	// The exit code for the error when exiting roachprod.
	ExitCode() int
}

// Exit codes for the errors
const (
	cmdExitCode          = 20
	sshExitCode          = 10
	unclassifiedExitCode = 1
)

// Cmd wraps errors that result from a command run against the cluster.
type Cmd struct {
	Err error
}

func (e Cmd) Error() string {
	return fmt.Sprintf("COMMAND_PROBLEM: %s", e.Err.Error())
}

// ExitCode gives the process exit code to return for non-kwbase command
// errors.
func (e Cmd) ExitCode() int {
	return cmdExitCode
}

// Format passes formatting responsibilities to kwbasedb/errors
func (e Cmd) Format(s fmt.State, verb rune) {
	errors.FormatError(e, s, verb)
}

// Unwrap the wrapped the non-kwbase command error.
func (e Cmd) Unwrap() error {
	return e.Err
}

// SSH wraps ssh-specific errors from connections to remote hosts.
type SSH struct {
	Err error
}

func (e SSH) Error() string {
	return fmt.Sprintf("SSH_PROBLEM: %s", e.Err.Error())
}

// ExitCode gives the process exit code to return for SSH errors.
func (e SSH) ExitCode() int {
	return sshExitCode
}

// Format passes formatting responsibilities to kwbasedb/errors
func (e SSH) Format(s fmt.State, verb rune) {
	errors.FormatError(e, s, verb)
}

// Unwrap the wrapped SSH error.
func (e SSH) Unwrap() error {
	return e.Err
}

// Unclassified wraps roachprod and unclassified errors.
type Unclassified struct {
	Err error
}

func (e Unclassified) Error() string {
	return fmt.Sprintf("UNCLASSIFIED_PROBLEM: %s", e.Err.Error())
}

// ExitCode gives the process exit code to return for unclassified errors.
func (e Unclassified) ExitCode() int {
	return unclassifiedExitCode
}

// Format passes formatting responsibilities to kwbasedb/errors
func (e Unclassified) Format(s fmt.State, verb rune) {
	errors.FormatError(e, s, verb)
}

// Unwrap the wrapped unclassified error.
func (e Unclassified) Unwrap() error {
	return e.Err
}

// ClassifyCmdError classifies an error received while executing a
// non-kwbase command remotely over an ssh connection to the right Error
// type.
func ClassifyCmdError(err error) Error {
	if err == nil {
		return nil
	}

	if exitErr, ok := asExitError(err); ok {
		if exitErr.ExitCode() == 255 {
			return SSH{err}
		}
		return Cmd{err}
	}

	return Unclassified{err}
}

// Extract the an ExitError from err's error tree or (nil, false) if none exists.
func asExitError(err error) (*exec.ExitError, bool) {
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr, true
	}
	return nil, false
}

// AsError extracts the Error from err's error tree or (nil, false) if none exists.
func AsError(err error) (Error, bool) {
	var e Error
	if errors.As(err, &e) {
		return e, true
	}
	return nil, false
}

// SelectPriorityError selects an error from the list in this priority order:
//
// - the Error with the highest exit code
// - one of the `error`s
// - nil
func SelectPriorityError(errors []error) error {
	var result Error
	for _, err := range errors {
		if err == nil {
			continue
		}

		rpErr, _ := AsError(err)
		if result == nil {
			result = rpErr
			continue
		}

		if rpErr.ExitCode() > result.ExitCode() {
			result = rpErr
		}
	}

	if result != nil {
		return result
	}

	for _, err := range errors {
		if err != nil {
			return err
		}
	}
	return nil
}
