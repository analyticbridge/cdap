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

package co.cask.cdap.proto.metadata;

import co.cask.cdap.proto.Id;

import javax.annotation.Nullable;

/**
 * Represents a Metadata change for a given {@link Id.NamespacedId}, including its previous state, the change,
 * the time that the change occurred and (optionally) the entity that made the update.
 */
public final class MetadataChangeRecord {
  private final MetadataRecord previous;
  private final MetadataDiffRecord changes;
  private final long updateTime;
  private final String updater;

  public MetadataChangeRecord(MetadataRecord previous, MetadataDiffRecord changes,
                              long updateTime) {
    this(previous, changes, updateTime, null);
  }

  public MetadataChangeRecord(MetadataRecord previous, MetadataDiffRecord changes,
                              long updateTime, @Nullable String updater) {
    this.previous = previous;
    this.changes = changes;
    this.updateTime = updateTime;
    this.updater = updater;
  }

  public MetadataRecord getPrevious() {
    return previous;
  }

  public MetadataDiffRecord getChanges() {
    return changes;
  }

  public long getUpdateTime() {
    return updateTime;
  }

  @Nullable
  public String getUpdater() {
    return updater;
  }

  /**
   * Represents the changes between the previous and the new record
   */
  public static final class MetadataDiffRecord {
    private final MetadataRecord additions;
    private final MetadataRecord deletions;

    public MetadataDiffRecord(MetadataRecord additions, MetadataRecord deletions) {
      this.additions = additions;
      this.deletions = deletions;
    }

    public MetadataRecord getAdditions() {
      return additions;
    }

    public MetadataRecord getDeletions() {
      return deletions;
    }
  }
}