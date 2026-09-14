// --------------------------------------------------------------------
// Copyright (c) 2026, WSO2 LLC. (https://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
// --------------------------------------------------------------------

package it

import "testing"

// TestSyncTopicName_MatchesProductionHash guards the one thing syncTopicName
// can get wrong without failing loudly: reading a queue that exists but belongs
// to nothing, which reports zero subscribers and looks like a driver bug.
//
// The expected hash was taken from a live run — the sync topic the gateway
// itself created for the asb-restart scenario. It pins both the hashing scheme
// and the arguments: the API's metadata name, not its display name, and the
// spec version.
func TestSyncTopicName_MatchesProductionHash(t *testing.T) {
	const (
		apiName = "asb-restart-v1-0"
		version = "v1.0"
		want    = "e90befc80b41625f17d070814c6412e2e344bc73aa6fc6f16c88990d14952d5c"
	)

	if got := syncTopicName(apiName, version); got != want {
		t.Fatalf("syncTopicName(%q, %q) = %q, want %q\n"+
			"the derivation has drifted from gateway-runtime's binding.WebSubApiSubscriptionTopic",
			apiName, version, got, want)
	}
}

// TestJoinNormalizedTopic_LengthPrefixPreventsCollision covers why the parts
// are length-prefixed rather than concatenated: without it, ("ab", "c") and
// ("a", "bc") would hash identically.
func TestJoinNormalizedTopic_LengthPrefixPreventsCollision(t *testing.T) {
	if joinNormalizedTopic("ab", "c") == joinNormalizedTopic("a", "bc") {
		t.Fatal("parts that regroup across a boundary must not produce the same topic")
	}
}

// TestCacheSubscriptionName_MatchesDriverNaming pins the derivation against the
// driver's cacheSubscriptionName. A drifting copy would delete or poll a
// subscription nobody owns, and the self-heal scenario would pass or fail for
// reasons unrelated to the driver.
func TestCacheSubscriptionName_MatchesDriverNaming(t *testing.T) {
	// "cache-" + first 16 hex chars of SHA-256("egw-it-asb"), the runtime ID
	// docker-compose.test.asb.yaml pins.
	const want = "cache-278e2481b26b414f"

	if got := cacheSubscriptionName("egw-it-asb"); got != want {
		t.Fatalf("cacheSubscriptionName = %q, want %q\n"+
			"derivation has drifted from the driver's cacheSubscriptionName", got, want)
	}
	if len(want) > 50 {
		t.Fatalf("name is %d chars, over Azure's 50-character subscription limit", len(want))
	}
}

// TestITRuntimeID_DefaultsToTheComposeValue guards the other half: the test and
// the gateway must agree on the identity, or the name is right and the owner is
// wrong.
func TestITRuntimeID_DefaultsToTheComposeValue(t *testing.T) {
	t.Setenv("APIP_EGW_RUNTIME_ID", "")
	if got := itRuntimeID(); got != defaultITRuntimeID {
		t.Fatalf("itRuntimeID() = %q, want the compose default %q", got, defaultITRuntimeID)
	}

	t.Setenv("APIP_EGW_RUNTIME_ID", "override-me")
	if got := itRuntimeID(); got != "override-me" {
		t.Fatalf("itRuntimeID() = %q, want the environment override to win", got)
	}
}
