use crate::context::{Ctx, Job};
use crate::nodes::{Branch, Children, End, Node, OneOf, ProvideStatus, Root, Task};
use crate::schemas::Parent;
use crate::status::{
    Completed, Status, all_parents_completed, all_parents_failed_or_skipped, find_completed_parent,
    some_parents_failed_or_skipped,
};

use std::collections::{HashSet, VecDeque};

pub trait Visitor<T> {
    fn visit_root(&mut self, node: &Root, ctx: &mut Ctx) -> T;
    fn visit_end(&mut self, node: &End, ctx: &mut Ctx) -> T;
    fn visit_branch(&mut self, node: &Branch, ctx: &mut Ctx) -> T;
    fn visit_task(&mut self, node: &Task, ctx: &mut Ctx) -> T;
    fn visit_oneof(&mut self, node: &OneOf, ctx: &mut Ctx) -> T;
    fn propagate(&self, node: &Node, queue: &mut VecDeque<usize>, visited: &HashSet<usize>) {
        for child_uid in node.children() {
            if !visited.contains(child_uid) {
                queue.push_back(*child_uid);
            }
        }
    }
}

pub struct NodeVisitor<'a> {
    pub nodes: &'a [Node],
}

impl<'a> Visitor<()> for NodeVisitor<'a> {
    fn visit_root(&mut self, node: &Root, ctx: &mut Ctx) {
        let parent_statuses = self.get_parent_statuses(&node.parents, &ctx.statuses);
        if all_parents_completed(&parent_statuses) {
            ctx.statuses[node.uid] = Status::Completed(Completed::Generic);
        } else if some_parents_failed_or_skipped(&parent_statuses) {
            ctx.statuses[node.uid] = Status::Skipped
        }
    }

    fn visit_end(&mut self, node: &End, ctx: &mut Ctx) {
        let parent_statuses = self.get_parent_statuses(&node.parents, &ctx.statuses);
        if all_parents_completed(&parent_statuses) {
            ctx.statuses[node.uid] = Status::Completed(Completed::Generic);
        } else if parent_statuses.iter().all(|s| s.is_final()) {
            ctx.statuses[node.uid] = Status::Skipped
        }
    }

    fn visit_branch(&mut self, node: &Branch, ctx: &mut Ctx) {
        let parent_statuses = self.get_parent_statuses(&node.parents, &ctx.statuses);
        if all_parents_completed(&parent_statuses) {
            ctx.jobs.push_back(Job::Branch(node.uid));
            ctx.statuses[node.uid] = Status::ReadyForSubmission;
        } else if some_parents_failed_or_skipped(&parent_statuses) {
            ctx.statuses[node.uid] = Status::Skipped;
        }
    }

    fn visit_task(&mut self, node: &Task, ctx: &mut Ctx) {
        let status = &ctx.statuses[node.uid];
        log::debug!("Visiting task '{}', status {}", node.uid, status);
        if let Status::Pending(_) | Status::Running(_) = status {
            return;
        }

        if let Status::ReadyForSubmission = status {
            log::debug!("Task '{}': Ready for submission", node.uid);
            ctx.jobs.push_back(Job::Task(node.uid));
            return;
        }

        let parent_statuses = self.get_parent_statuses(&node.parents, &ctx.statuses);
        if all_parents_completed(&parent_statuses) {
            if node.cache | node.cache_local {
                log::debug!("Task '{}': Pushing cache validation", node.uid);
                ctx.jobs.push_back(Job::ValidateCache(node.uid));
            } else {
                log::debug!("Task '{}': Pushing task execution", node.uid);
                ctx.jobs.push_back(Job::Task(node.uid));
                ctx.statuses[node.uid] = Status::ReadyForSubmission;
            }
        } else if some_parents_failed_or_skipped(&parent_statuses) {
            log::debug!("Task '{}' is skipped", node.uid);
            ctx.statuses[node.uid] = Status::Skipped
        }
    }

    fn visit_oneof(&mut self, node: &OneOf, ctx: &mut Ctx) {
        let parent_statuses = self.get_parent_statuses(&node.parents, &ctx.statuses);
        if let Some(uid) = find_completed_parent(&node.parents, &ctx.statuses) {
            ctx.jobs.push_back(Job::OneOf(node.uid, uid));
            ctx.statuses[node.uid] = Status::ReadyForSubmission
        } else if all_parents_failed_or_skipped(&parent_statuses) {
            ctx.statuses[node.uid] = Status::Skipped
        }
    }
}

impl<'a> NodeVisitor<'a> {
    pub fn visit(&mut self, ctx: &mut Ctx) {
        let mut visited = HashSet::new();
        while let Some(uid) = ctx.updated.pop_front() {
            if !visited.insert(uid) {
                continue;
            }
            let node = &self.nodes[uid];
            let status = &ctx.statuses[uid];
            if !status.is_final() {
                match node {
                    Node::Root(node) => self.visit_root(&node, ctx),
                    Node::End(node) => self.visit_end(&node, ctx),
                    Node::Branch(node) => self.visit_branch(&node, ctx),
                    Node::OneOf(node) => self.visit_oneof(&node, ctx),
                    Node::Task(node) => self.visit_task(&node, ctx),
                }
            }
            self.propagate(node, &mut ctx.updated, &mut visited);
        }
    }

    fn get_parent_statuses<'b>(
        &self,
        parents: &[Parent],
        statuses: &'b [Status],
    ) -> Vec<&'b Status> {
        parents
            .iter()
            .map(|parent| (&self.nodes[parent.uid], &statuses[parent.uid], &parent.kind))
            .map(|(node, status, kind)| node.provide_status(status, kind))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schemas::{Cmd, ExecMode, ParentKind, Script, ScriptPath, SlurmOverride};
    use crate::status::{Failed, JobType};

    fn get_ctx(nodes: &[Node]) -> Ctx {
        Ctx::new(nodes).unwrap()
    }

    fn get_root(uid: usize) -> Root {
        Root {
            uid,
            pipeline_name: "pipeline".into(),
            parents: vec![],
            children: vec![],
        }
    }

    fn get_end(uid: usize) -> End {
        End {
            uid,
            pipeline_name: "pipeline".into(),
            parents: vec![],
            children: vec![],
            artifacts: vec![],
        }
    }

    fn get_branch(
        uid: usize,
        parent_uid: usize,
        true_branch: usize,
        false_branch: usize,
    ) -> Branch {
        Branch {
            uid,
            pipeline_name: "pipeline".into(),
            parents: vec![Parent {
                uid: parent_uid,
                kind: ParentKind::Output {
                    key: "choice".into(),
                },
            }],
            children: vec![true_branch, false_branch],
            artifacts: vec![],
        }
    }

    fn get_parent(uid: usize) -> Parent {
        Parent {
            uid,
            kind: ParentKind::Logical,
        }
    }

    fn get_branch_parent(uid: usize, branch: bool) -> Parent {
        Parent {
            uid,
            kind: ParentKind::Branch { branch },
        }
    }

    fn get_oneof(uid: usize, parent1: usize, parent2: usize) -> OneOf {
        OneOf {
            uid,
            pipeline_name: "pipeline".into(),
            parents: vec![get_parent(parent1), get_parent(parent2)],
            children: vec![],
            artifacts: vec![],
        }
    }

    fn get_task(uid: usize) -> Task {
        Task {
            uid,
            parents: vec![],
            fn_name: "fn_name".into(),
            name: "name".into(),
            pipeline_name: "pipeline_name".into(),
            cache: false,
            cache_local: false,
            cache_ignore: vec![],
            debug: false,
            mode: ExecMode::Wrap,
            cmd: Cmd::Sbatch,
            retries: 0,
            script: Script::ScriptPath(ScriptPath {
                path: "path/to/script".into(),
            }),
            kwargs: vec![],
            artifacts: vec![],
            children: vec![],
            slurm_override: SlurmOverride::new(),
        }
    }

    #[test]
    fn test_visit_root_completed() {
        let root = get_root(0);
        let nodes = [Node::Root(root)];

        let mut ctx = get_ctx(&nodes);
        ctx.updated.push_back(0);

        let mut visitor = NodeVisitor { nodes: &nodes };
        visitor.visit(&mut ctx);

        assert!(matches!(
            ctx.statuses[0],
            Status::Completed(Completed::Generic)
        ))
    }

    #[test]
    fn test_visit_root_skipped() {
        let root1 = get_root(0);
        let mut root2 = get_root(1);
        root2.parents.push(get_parent(0));
        let nodes = [Node::Root(root1), Node::Root(root2)];

        let mut ctx = get_ctx(&nodes);
        ctx.updated.push_back(1);
        ctx.statuses[0] = Status::Failed(Failed::Generic);

        let mut visitor = NodeVisitor { nodes: &nodes };
        visitor.visit(&mut ctx);

        assert!(matches!(ctx.statuses[1], Status::Skipped))
    }

    #[test]
    fn test_visit_end_completed() {
        let end = get_end(0);
        let nodes = [Node::End(end)];

        let mut ctx = get_ctx(&nodes);
        ctx.updated.push_back(0);

        let mut visitor = NodeVisitor { nodes: &nodes };
        visitor.visit(&mut ctx);

        assert!(matches!(
            ctx.statuses[0],
            Status::Completed(Completed::Generic)
        ))
    }

    #[test]
    fn test_visit_end_skipped() {
        let root = get_root(0);
        let mut end = get_end(1);
        end.parents.push(get_parent(0));
        let nodes = [Node::Root(root), Node::End(end)];

        let mut ctx = get_ctx(&nodes);
        ctx.updated.push_back(1);
        ctx.statuses[0] = Status::Failed(Failed::Generic);

        let mut visitor = NodeVisitor { nodes: &nodes };
        visitor.visit(&mut ctx);

        assert!(matches!(ctx.statuses[1], Status::Skipped))
    }

    #[test]
    fn test_visit_end_not_submitted() {
        let root = get_root(0);
        let root2 = get_root(1);
        let mut end = get_end(2);
        end.parents.push(get_parent(0));
        end.parents.push(get_parent(1));
        let nodes = [Node::Root(root), Node::Root(root2), Node::End(end)];

        let mut ctx = get_ctx(&nodes);
        ctx.updated.push_back(2);
        ctx.statuses[0] = Status::Failed(Failed::Generic);
        ctx.statuses[1] = Status::Running(JobType::Slurm("123".into()));

        let mut visitor = NodeVisitor { nodes: &nodes };
        visitor.visit(&mut ctx);

        assert!(matches!(ctx.statuses[2], Status::NotSubmitted))
    }

    #[test]
    fn test_visit_end_skipped_mixed() {
        let root = get_root(0);
        let root2 = get_root(1);
        let mut end = get_end(2);
        end.parents.push(get_parent(0));
        end.parents.push(get_parent(1));
        let nodes = [Node::Root(root), Node::Root(root2), Node::End(end)];

        let mut ctx = get_ctx(&nodes);
        ctx.updated.push_back(2);
        ctx.statuses[0] = Status::Failed(Failed::Generic);
        ctx.statuses[1] = Status::Completed(Completed::Generic);

        let mut visitor = NodeVisitor { nodes: &nodes };
        visitor.visit(&mut ctx);

        assert!(matches!(ctx.statuses[2], Status::Skipped))
    }

    #[test]
    fn test_visit_branch_ready() {
        let root = get_root(0);

        let mut end1 = get_end(2);
        end1.parents.push(Parent {
            uid: 1,
            kind: ParentKind::Branch { branch: true },
        });

        let mut end2 = get_end(3);
        end2.parents.push(Parent {
            uid: 1,
            kind: ParentKind::Branch { branch: false },
        });

        let branch = get_branch(1, 0, 2, 3);
        let nodes = [
            Node::Root(root),
            Node::Branch(branch),
            Node::End(end1),
            Node::End(end2),
        ];

        let mut ctx = get_ctx(&nodes);
        ctx.updated.push_back(1);
        ctx.statuses[0] = Status::Completed(Completed::Generic);

        let mut visitor = NodeVisitor { nodes: &nodes };
        visitor.visit(&mut ctx);

        assert!(matches!(ctx.statuses[1], Status::ReadyForSubmission));
        assert!(matches!(ctx.statuses[2], Status::NotSubmitted));
        assert!(matches!(ctx.statuses[3], Status::NotSubmitted));
    }

    #[test]
    fn test_visit_branch_skipped() {
        let root = get_root(0);
        let mut end1 = get_end(2);
        let mut end2 = get_end(3);
        let branch = get_branch(1, 0, 2, 3);

        end1.parents.push(get_branch_parent(1, true));
        end2.parents.push(get_branch_parent(1, false));

        let nodes = [
            Node::Root(root),
            Node::Branch(branch),
            Node::End(end1),
            Node::End(end2),
        ];

        let mut ctx = get_ctx(&nodes);
        ctx.updated.push_back(1);
        ctx.statuses[0] = Status::Failed(Failed::Generic);
        let mut visitor = NodeVisitor { nodes: &nodes };
        visitor.visit(&mut ctx);

        assert!(matches!(ctx.statuses[1], Status::Skipped));
        assert!(matches!(ctx.statuses[2], Status::Skipped));
        assert!(matches!(ctx.statuses[3], Status::Skipped));
    }

    #[test]
    fn test_visit_branch_completed() {
        let root = get_root(0);
        let mut end2 = get_end(3);
        let mut end1 = get_end(2);
        let branch = get_branch(1, 0, 2, 3);

        end1.parents.push(get_branch_parent(1, true));
        end2.parents.push(get_branch_parent(1, false));

        let nodes = [
            Node::Root(root),
            Node::Branch(branch),
            Node::End(end1),
            Node::End(end2),
        ];

        let mut ctx = get_ctx(&nodes);
        ctx.updated.push_back(1);
        ctx.statuses[0] = Status::Completed(Completed::Generic);
        ctx.statuses[1] = Status::Completed(Completed::Branch(true));

        let mut visitor = NodeVisitor { nodes: &nodes };
        visitor.visit(&mut ctx);

        assert!(matches!(
            ctx.statuses[2],
            Status::Completed(Completed::Generic)
        ));
        assert!(matches!(ctx.statuses[3], Status::Skipped));
    }

    #[test]
    fn test_oneof_ready() {
        let root1 = get_root(0);
        let root2 = get_root(1);
        let mut oneof = get_oneof(2, 0, 1);

        oneof.parents.push(get_parent(0));
        oneof.parents.push(get_parent(1));

        let nodes = [Node::Root(root1), Node::Root(root2), Node::OneOf(oneof)];

        let mut ctx = get_ctx(&nodes);
        ctx.updated.push_back(2);
        ctx.statuses[0] = Status::Completed(Completed::Generic);
        ctx.statuses[1] = Status::Failed(Failed::Generic);

        let mut visitor = NodeVisitor { nodes: &nodes };
        visitor.visit(&mut ctx);
        assert!(matches!(ctx.statuses[2], Status::ReadyForSubmission));
    }

    #[test]
    fn test_oneof_skipped() {
        let root1 = get_root(0);
        let root2 = get_root(1);
        let mut oneof = get_oneof(2, 0, 1);

        oneof.parents.push(get_parent(0));
        oneof.parents.push(get_parent(1));

        let nodes = [Node::Root(root1), Node::Root(root2), Node::OneOf(oneof)];

        let mut ctx = get_ctx(&nodes);
        ctx.updated.push_back(2);
        ctx.statuses[0] = Status::Skipped;
        ctx.statuses[1] = Status::Failed(Failed::Generic);

        let mut visitor = NodeVisitor { nodes: &nodes };
        visitor.visit(&mut ctx);
        assert!(matches!(ctx.statuses[2], Status::Skipped));
    }

    #[test]
    fn test_visit_task_skipped() {
        let root1 = get_root(0);
        let root2 = get_root(1);
        let mut task = get_task(2);

        task.parents.push(get_parent(0));
        task.parents.push(get_parent(1));

        let nodes = [Node::Root(root1), Node::Root(root2), Node::Task(task)];

        let mut ctx = get_ctx(&nodes);
        ctx.updated.push_back(2);
        ctx.statuses[0] = Status::Completed(Completed::Generic);
        ctx.statuses[1] = Status::Failed(Failed::Generic);

        let mut visitor = NodeVisitor { nodes: &nodes };
        visitor.visit(&mut ctx);

        assert!(matches!(ctx.statuses[2], Status::Skipped));
        assert_eq!(ctx.jobs.len(), 0);
    }

    #[test]
    fn test_visit_task_ready() {
        let root1 = get_root(0);
        let root2 = get_root(1);
        let mut task = get_task(2);

        task.parents.push(get_parent(0));
        task.parents.push(get_parent(1));

        let nodes = [Node::Root(root1), Node::Root(root2), Node::Task(task)];

        let mut ctx = get_ctx(&nodes);
        ctx.updated.push_back(2);
        ctx.statuses[0] = Status::Completed(Completed::Generic);
        ctx.statuses[1] = Status::Completed(Completed::Cached);

        let mut visitor = NodeVisitor { nodes: &nodes };
        visitor.visit(&mut ctx);

        assert!(matches!(ctx.statuses[2], Status::ReadyForSubmission));

        let job = ctx.jobs.pop_front().unwrap();
        assert!(matches!(job, Job::Task(2)));
    }

    #[test]
    fn test_visit_task_cache() {
        let root1 = get_root(0);
        let root2 = get_root(1);
        let mut task = get_task(2);

        task.parents.push(get_parent(0));
        task.parents.push(get_parent(1));
        task.cache = true;

        let nodes = [Node::Root(root1), Node::Root(root2), Node::Task(task)];

        let mut ctx = get_ctx(&nodes);
        ctx.updated.push_back(2);
        ctx.statuses[0] = Status::Completed(Completed::Generic);
        ctx.statuses[1] = Status::Completed(Completed::Cached);

        let mut visitor = NodeVisitor { nodes: &nodes };
        visitor.visit(&mut ctx);

        let job = ctx.jobs.pop_front().unwrap();
        assert!(matches!(job, Job::ValidateCache(2)));
    }
}
