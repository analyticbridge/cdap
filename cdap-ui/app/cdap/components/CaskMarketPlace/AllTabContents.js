/*
 * Copyright © 2016 Cask Data, Inc.
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

import React, {Component} from 'react';
import SearchTextBox from '../SearchTextBox';
import MarketPlaceEntity from '../MarketPlaceEntity';
import T from 'i18n-react';
import MarketStore from './store/market-store.js';
import Fuse from 'fuse.js';
require('./AllTabContents.less');

export default class AllTabContents extends Component {
  constructor(props) {
    super(props);
    this.state = {
      searchStr: '',
      entities: [],
      loading: MarketStore.getState().loading
    };

    this.unsub = MarketStore.subscribe(() => {
      this.filterEntities();
      const loading = MarketStore.getState().loading;
      this.setState({loading});
    });
  }

  componentWillUnmount () {
    this.unsub();
    MarketStore.dispatch({type: 'RESET'});
  }

  filterEntities() {
    const {list, filter} = MarketStore.getState();
    if (filter === '*') {
      this.setState({entities: list});
      return;
    }

    const fuseOptions = {
      caseSensitive: true,
      threshold: 0,
      location: 0,
      distance: 100,
      maxPatternLength: 32,
      keys: [
        "categories"
      ]
    };

    let fuse = new Fuse(list, fuseOptions);
    let search = fuse.search(filter);

    this.setState({entities: search});
  }

  onSearch(changeEvent) {
    // For now just save. Eventually we will make a backend call to get the search result.
    this.setState({searchStr: changeEvent.target.value});
  }

  generateIconPath(entity) {
    return `http://marketplace.cask.co.s3.amazonaws.com/packages/${entity.name}/${entity.version}/icon.jpg`;
  }

  render() {
    const loadingElem = (
      <h4>
        <span className="fa fa-refresh fa-spin"></span>
      </h4>
    );

    return (
      <div className="all-tab-content">
        <SearchTextBox
          placeholder={T.translate('features.Market.search-placeholder')}
          value={this.state.searchStr}
          onChange={this.onSearch.bind(this)}
        />
        <div className="body-section">
          {
            this.state.loading ? loadingElem :
            this.state.entities.length === 0 ?
            ( <h3>Empty</h3> ) :
            this.state.entities
              .map((e, index) => (
                <MarketPlaceEntity
                  name={e.name}
                  subtitle={e.version}
                  key={index}
                  icon={this.generateIconPath(e)}
                  size="medium"
                />
              ))
          }
        </div>
      </div>
    );
  }
}