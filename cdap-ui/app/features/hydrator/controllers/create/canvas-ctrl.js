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

class HydratorCreateCanvasController {
  constructor(BottomPanelStore, NodesStore, NodesActionsFactory, ConfigStore, PipelineNodeConfigActionFactory) {
    this.NodesStore = NodesStore;
    this.ConfigStore = ConfigStore;
    this.PipelineNodeConfigActionFactory = PipelineNodeConfigActionFactory;
    this.NodesActionsFactory = NodesActionsFactory;

    this.setState = () => {
      this.state = {
        setScroll: (BottomPanelStore.getPanelState() === 0? false: true)
      };
    };
    this.setState();
    BottomPanelStore.registerOnChangeListener(this.setState.bind(this));

    this.nodes = [];
    this.connections = [];

    this.updateNodesAndConnections();
    NodesStore.registerOnChangeListener(this.updateNodesAndConnections.bind(this));

  }

  setStateAndUpdateConfigStore() {
    this.nodes = this.NodesStore.getNodes();
    this.connections = this.NodesStore.getConnections();
    this.ConfigStore.setNodes(this.nodes);
    this.ConfigStore.setConnections(this.connections);
  }

  updateNodesAndConnections() {
    this.setStateAndUpdateConfigStore();
    var activeNode = this.NodesStore.getActiveNodeId();
    if (!activeNode) {
      this.deleteNode();
    } else {
      this.setActiveNode();
    }
  }

  setActiveNode() {
    var nodeId = this.NodesStore.getActiveNodeId();
    if (!nodeId) {
      return;
    }
    var plugin;
    var nodeFromNodesStore;
    var nodeFromConfigStore = this.ConfigStore.getNodes().filter( node => node.id === nodeId );
    if (nodeFromConfigStore.length) {
      plugin = nodeFromConfigStore[0];
    } else {
      nodeFromNodesStore = this.NodesStore.getNodes().filter(node => node.id === nodeId);
      plugin = nodeFromNodesStore[0];
    }
    this.PipelineNodeConfigActionFactory.choosePlugin(plugin);
  }

  deleteNode() {
    this.setStateAndUpdateConfigStore();
    this.PipelineNodeConfigActionFactory.removePlugin();
  }
}


HydratorCreateCanvasController.$inject = ['BottomPanelStore', 'NodesStore', 'NodesActionsFactory', 'ConfigStore', 'PipelineNodeConfigActionFactory'];
angular.module(PKG.name + '.feature.hydrator')
  .controller('HydratorCreateCanvasController', HydratorCreateCanvasController);
