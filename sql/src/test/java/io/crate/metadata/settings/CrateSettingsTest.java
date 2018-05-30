/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.metadata.settings;

import io.crate.expression.NestableInput;
import io.crate.execution.engine.collect.stats.JobsLogService;
import io.crate.expression.reference.NestedObjectExpression;
import io.crate.settings.CrateSetting;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayService;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

public class CrateSettingsTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testValidSetting() {
        assertThat(CrateSettings.isValidSetting(JobsLogService.STATS_ENABLED_SETTING.getKey()), is(true));
    }

    @Test
    public void testValidLoggingSetting() {
        assertThat(CrateSettings.isValidSetting("logger.info"), is(true));
    }

    @Test
    public void testValidPrefixSetting() {
        assertThat(CrateSettings.isValidSetting("stats"), is(true));
    }

    @Test
    public void testSettingsByNamePrefix() {
        assertThat(CrateSettings.settingNamesByPrefix("stats.jobs_log"), containsInAnyOrder(
            JobsLogService.STATS_JOBS_LOG_SIZE_SETTING.getKey(),
            JobsLogService.STATS_JOBS_LOG_EXPIRATION_SETTING.getKey()
            ));
    }

    @Test
    public void testLoggingSettingsByNamePrefix() throws Exception {
        assertThat(CrateSettings.settingNamesByPrefix("logger."), contains("logger."));
    }

    @Test
    public void testIsRuntimeSetting() {
        // valid, no exception thrown here
        CrateSettings.checkIfRuntimeSetting(JobsLogService.STATS_ENABLED_SETTING.getKey());
    }

    @Test
    public void testIsNotRuntimeSetting() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Setting 'gateway.expected_nodes' cannot be set/reset at runtime");

        CrateSettings.checkIfRuntimeSetting(GatewayService.EXPECTED_NODES_SETTING.getKey());
    }

    @Test
    public void testFlattenObjectSettings() {
        Map<String, Object> value = MapBuilder.<String, Object>newMapBuilder()
            .put("enabled", true)
            .put("breaker",
                MapBuilder.newMapBuilder()
                    .put("log", MapBuilder.newMapBuilder()
                        .put("jobs", MapBuilder.newMapBuilder()
                            .put("overhead", 1.05d).map()
                        ).map()
                    ).map()
            ).map();

        Settings.Builder builder  = Settings.builder();
        Settings expected = Settings.builder()
            .put("stats.enabled", true)
            .put("stats.breaker.log.jobs.overhead", 1.05d)
            .build();
        CrateSettings.flattenSettings(builder, "stats", value);
        assertThat(builder.build(), is(expected));
    }

    @Test
    public void testFlattenSettingsConvertsBytesRef() {
        Settings.Builder builder  = Settings.builder();
        CrateSettings.flattenSettings(builder, "dummy", new BytesRef("foo"));
        assertThat(builder.get("dummy"), is("foo"));
    }

    @Test
    public void testDefaultValuesAreSet() {
        CrateSettings crateSettings = new CrateSettings(clusterService, clusterService.getSettings());
        assertThat(
            crateSettings.settings().get(JobsLogService.STATS_ENABLED_SETTING.getKey()),
            is(JobsLogService.STATS_ENABLED_SETTING.setting().getDefaultRaw(Settings.EMPTY)));
    }

    @Test
    public void testReferenceMapIsBuild() {
        CrateSettings crateSettings = new CrateSettings(clusterService, clusterService.getSettings());
        NestedObjectExpression stats = (NestedObjectExpression) crateSettings.referenceImplementationTree().get("stats");
        CrateSettings.SettingExpression statsEnabled = (CrateSettings.SettingExpression) stats.getChild("enabled");
        assertThat(statsEnabled.name(), is("enabled"));
        assertThat(statsEnabled.value(), is(true));
    }

    @Test
    public void testSettingsChanged() {
        CrateSettings crateSettings = new CrateSettings(clusterService, clusterService.getSettings());

        ClusterState newState = ClusterState.builder(clusterService.state())
            .metaData(MetaData.builder().transientSettings(
                Settings.builder().put(JobsLogService.STATS_ENABLED_SETTING.getKey(), true).build()))
            .build();
        crateSettings.clusterChanged(new ClusterChangedEvent("settings updated", newState, clusterService.state()));
        assertThat(
            crateSettings.settings().getAsBoolean(JobsLogService.STATS_ENABLED_SETTING.getKey(), false),
            is(true));
    }


    @Test
    public void testBuildGroupSettingReferenceTree() {
        // create groupSetting to be used in tests
        CrateSetting<Settings> TEST_SETTING = CrateSetting.of(
            Setting.groupSetting("test.",
                Setting.Property.Dynamic,
                Setting.Property.NodeScope),
            DataTypes.OBJECT
        );

        Settings initialSettings = Settings.builder()
            .put("test.setting.a.user", "42")
            .put("test.setting.a.method", "something")
            .put("test.setting.b", "value-b")
            .put("test.setting.c", "value-c")
            .build();
        CrateSettings crateSettings = new CrateSettings(clusterService, initialSettings);

        Map<String, Settings> settingsMap = initialSettings.getGroups(TEST_SETTING.getKey(), true);
        //build reference Map for TEST_SETTING
        Map<String, NestableInput> referenceMap = new HashMap<>(4);
        for (Map.Entry<String, Settings> entry : settingsMap.entrySet()) {
            crateSettings.buildGroupSettingReferenceTree(TEST_SETTING.getKey(), entry.getKey(), entry.getValue(),
                referenceMap);
        }

        NestedObjectExpression test = (NestedObjectExpression) referenceMap.get("test");
        NestedObjectExpression testSetting = (NestedObjectExpression) test.getChild("setting");
        assertThat(testSetting.value().containsKey("a"), is(true));
        assertThat(testSetting.value().containsKey("b"), is(true));
        assertThat(testSetting.value().containsKey("c"), is(true));

        CrateSettings.NestedSettingExpression a = (CrateSettings.NestedSettingExpression) testSetting.getChild("a");
        assertThat(a.value().containsKey("method"), is(true));
        assertThat(a.value().containsKey("user"), is(true));
    }


    @Test
    public void testRecoverAfterTimeDefaultValueWithExpectedNodesSet() throws Exception {
        Settings settings = Settings.builder().put(GatewayService.EXPECTED_NODES_SETTING.getKey(), 1).build();
        CrateSettings crateSettings = new CrateSettings(clusterService, settings);

        NestableInput impl = crateSettings.referenceImplementationTree()
            .get("gateway")
            .getChild("recover_after_time");

        assertThat(((BytesRef) impl.value()).utf8ToString(), is("5m"));
    }

    @Test
    public void testRecoverAfterTimeDefaultValueWithoutExpectedNodesSet() throws Exception {
        CrateSettings crateSettings = new CrateSettings(clusterService, Settings.EMPTY);
        NestableInput impl = crateSettings.referenceImplementationTree()
            .get("gateway")
            .getChild("recover_after_time");
        assertThat(((BytesRef) impl.value()).utf8ToString(), is("0ms"));
    }
}
