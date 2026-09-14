use crate::nodes::{Node, Parents};

pub fn find_root_node(nodes: &[Node]) -> Option<usize> {
    for node in nodes {
        if let Node::Root(root) = node
            && root.parents.len() == 0
        {
            return Some(root.uid);
        }
    }

    None
}

pub fn add_children(nodes: &mut [Node]) {
    let mut all_children: Vec<Vec<usize>> = vec![Vec::new(); nodes.len()];
    for node in nodes.iter() {
        let uid = node.get_uid();
        for parent in node.parents() {
            all_children[parent].push(uid);
        }
    }

    for (uid, mut children) in all_children.into_iter().enumerate() {
        children.sort();
        children.dedup();
        nodes[uid].set_children(children);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::{End, Root, Task};
    use crate::schemas::{
        Cmd, ExecMode, Parent, ParentKind, Scope, Script, ScriptPath, SlurmOverride,
    };
    use std::collections::HashMap;

    fn get_nodes() -> Vec<Node> {
        let root = Root {
            uid: 0,
            pipeline_name: "pipeline".into(),
            parents: Vec::new(),
            children: Vec::new(),
        };
        let task = Task {
            uid: 1,
            parents: vec![Parent {
                uid: 0,
                kind: ParentKind::Logical,
            }],
            scope: Scope::Global,
            fn_name: "fn_name".into(),
            name: "name".into(),
            pipeline_name: "pipeline_name".into(),
            cache: true,
            cache_ignore: vec![],
            cache_size: 1,
            mode: ExecMode::Wrap,
            cmd: Cmd::Sbatch,
            envs: HashMap::new(),
            retries: 0,
            script: Script::ScriptPath(ScriptPath {
                path: "path/to/script".into(),
            }),
            tags: vec![],
            kwargs: vec![],
            artifacts: vec![],
            children: vec![],
            slurm: SlurmOverride::default(),
        };
        let end = End {
            uid: 2,
            pipeline_name: "pipeline".into(),
            parents: vec![Parent {
                uid: 1,
                kind: ParentKind::Logical,
            }],
            children: vec![],
            artifacts: vec![],
        };

        vec![Node::Root(root), Node::Task(task), Node::End(end)]
    }

    #[test]
    fn test_add_children() {
        let mut nodes = get_nodes();
        add_children(&mut nodes);

        let Node::Root(root) = &nodes[0] else {
            panic!();
        };
        assert_eq!(root.children, vec![1]);

        let Node::Task(task) = &nodes[1] else {
            panic!();
        };
        assert_eq!(task.children, vec![2]);

        let Node::End(end) = &nodes[2] else {
            panic!();
        };
        assert_eq!(end.children.len(), 0);
    }

    #[test]
    fn test_find_root() {
        let mut nodes = get_nodes();
        add_children(&mut nodes);
        let root_uid = find_root_node(&mut nodes).unwrap();
        assert_eq!(root_uid, 0)
    }
}
