//! Pipeline graph.
//!
//! To avoid Rc, the graph is represented as a uid -> node hashmap.

use crate::model::{DAG, JobStatus, Node, NodeResult};

use std::collections::HashMap;

/// Build the nodemap and return it alongside the root node unique id.
pub fn build_nodemap(dag: DAG<Node>) -> Option<(String, HashMap<String, Node>)> {
    let mut nodemap = HashMap::new();
    for node in dag.nodes {
        nodemap.insert(node.uid.clone(), node);
    }

    add_child_edges(&mut nodemap);
    let root_id = find_root_id(&nodemap)?;
    let root = nodemap.get_mut(&root_id)?;
    root.status = JobStatus::Completed(NodeResult::Node);
    Some((root_id, nodemap))
}

/// Find the uid of the (unique) root node.
fn find_root_id(nodemap: &HashMap<String, Node>) -> Option<String> {
    nodemap
        .values()
        .filter(|node| node.parents.len() == 0)
        .map(|node| &node.uid)
        .map(|uid| uid.to_string())
        .next()
}

/// Add children to nodes.
///
/// They are added so that the node state can be propagated
/// to its children.
fn add_child_edges(nodemap: &mut HashMap<String, Node>) {
    let mut edges = find_child_edges(nodemap);
    for (uid, node) in nodemap.iter_mut() {
        if let Some(children) = edges.remove(uid) {
            node.children.extend(children);
        }
    }
}

/// construct the child edges.
fn find_child_edges(nodemap: &HashMap<String, Node>) -> HashMap<String, Vec<String>> {
    let mut children: HashMap<String, Vec<String>> = HashMap::new();
    for (uid, node) in nodemap.iter() {
        for parent in &node.parents {
            let parent_vec = children.entry(parent.uid.clone()).or_default();
            if !parent_vec.contains(uid) {
                parent_vec.push(uid.clone())
            }
        }
    }
    children
}
