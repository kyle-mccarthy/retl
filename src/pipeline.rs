pub struct Pipeline {
    id: String,
    description: String,
    tasks: Vec<Task>,
}

pub enum TaskKind {
    Source,
    Op,
    Destination,
}

pub struct Task {
    id: String,
    description: String,
    kind: TaskKind,
}
