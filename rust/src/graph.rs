use crate::model::{DAG, JobStatus, Node, NodeResult};

use std::collections::HashMap;

pub fn build_nodemap(dag: DAG) -> Option<(String, HashMap<String, Node>)> {
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

fn find_root_id(nodemap: &HashMap<String, Node>) -> Option<String> {
    nodemap
        .values()
        .filter(|node| node.parents.len() == 0)
        .map(|node| &node.uid)
        .map(|uid| uid.to_string())
        .next()
}

fn add_child_edges(nodemap: &mut HashMap<String, Node>) {
    let mut edges = find_child_edges(nodemap);
    for (uid, node) in nodemap.iter_mut() {
        if let Some(children) = edges.remove(uid) {
            node.children.extend(children);
        }
    }
}

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
