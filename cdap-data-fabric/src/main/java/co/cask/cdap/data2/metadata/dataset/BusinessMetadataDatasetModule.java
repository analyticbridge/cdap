/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package co.cask.cdap.data2.metadata.dataset;

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.lib.IndexedTable;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;

/**
 * {@link co.cask.cdap.api.dataset.module.DatasetModule} for {@link BusinessMetadataDataset}.
 */
public class BusinessMetadataDatasetModule implements DatasetModule {

  @Override
  public void register(DatasetDefinitionRegistry registry) {
    DatasetDefinition<IndexedTable, ? extends DatasetAdmin> indexedTableDef =
      registry.get(IndexedTable.class.getName());
    registry.add(new BusinessMetadataDefinition("businessMetadataDataset", indexedTableDef));
    registry.add(new BusinessMetadataDefinition(BusinessMetadataDataset.class.getSimpleName(), indexedTableDef));
  }
}