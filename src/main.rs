use libp2p::{
    core::upgrade,
    floodsub::{Floodsub, FloodsubEvent, Topic},
    futures::StreamExt,
    identity,
    mdns::{Mdns, MdnsEvent},
    mplex,
    noise::{Keypair, NoiseConfig, X25519Spec},
    swarm::{NetworkBehaviourEventProcess, Swarm, SwarmBuilder},
    tcp::TokioTcpConfig,
    NetworkBehaviour, PeerId, Transport,
};
use log::{error, info};
use once_cell::sync::Lazy;
use opentelemetry::trace::TraceError;
use opentelemetry::{
    global,
    sdk::trace as sdktrace,
    trace::{FutureExt, TraceContextExt, Tracer},
    Context,
};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use tokio::{fs, io::AsyncBufReadExt, sync::mpsc};

const STORAGE_FILE_PATH: &str = "./documents.json";

static KEYS: Lazy<identity::Keypair> = Lazy::new(|| identity::Keypair::generate_ed25519());
static PEER_ID: Lazy<PeerId> = Lazy::new(|| PeerId::from(KEYS.public()));
static TOPIC: Lazy<Topic> = Lazy::new(|| Topic::new("docs"));

type Documents = Vec<Document>;

#[derive(Debug, Serialize, Deserialize)]
struct Document {
    id: usize,
    name: String,
    author: String,
    content: String,
    public: bool,
}

#[derive(Debug, Serialize, Deserialize)]
enum ListMode {
    ALL,         // List documents from all nodes
    One(String), // List documents from one node
}

#[derive(Debug, Serialize, Deserialize)]
struct ListRequest {
    mode: ListMode,
}

#[derive(Debug, Serialize, Deserialize)]
struct ListResponse {
    mode: ListMode,
    data: Documents,
    receiver: String,
}

enum EventType {
    Response(ListResponse), // from another peer
    Input(String),          // from this peer
}

/// This macro implements all the NetworkBehaviour trait's functions for all the members of
/// the struct, which are not annotated with behaviour(ignore)
#[derive(NetworkBehaviour)]
struct DocumentBehaviour {
    floodsub: Floodsub,
    mdns: Mdns,
    #[behaviour(ignore)]
    response_sender: mpsc::UnboundedSender<ListResponse>,
}

impl NetworkBehaviourEventProcess<FloodsubEvent> for DocumentBehaviour {
    fn inject_event(&mut self, event: FloodsubEvent) {
        match event {
            FloodsubEvent::Message(msg) => {
                if let Ok(resp) = serde_json::from_slice::<ListResponse>(&msg.data) {
                    if resp.receiver == PEER_ID.to_string() {
                        info!("Response from {}:", msg.source);
                        resp.data.iter().for_each(|r| info!("{:?}", r));
                    }
                } else if let Ok(req) = serde_json::from_slice::<ListRequest>(&msg.data) {
                    match req.mode {
                        ListMode::ALL => {
                            info!("Received ALL req: {:?} from {:?}", req, msg.source);
                            respond_with_public_docs(
                                self.response_sender.clone(),
                                msg.source.to_string(),
                            );
                        }
                        ListMode::One(ref peer_id) => {
                            if peer_id == &PEER_ID.to_string() {
                                info!("Received req: {:?} from {:?}", req, msg.source);
                                respond_with_public_docs(
                                    self.response_sender.clone(),
                                    msg.source.to_string(),
                                );
                            }
                        }
                    }
                }
            }
            _ => (),
        }
    }
}

/// Spawn to asynchronously execute a future, which reads all local documents and
/// sends this data through the channel_sender over to our event loop
fn respond_with_public_docs(sender: mpsc::UnboundedSender<ListResponse>, receiver: String) {
    tokio::spawn(async move {
        match read_local_docs().await {
            Ok(docs) => {
                let resp = ListResponse {
                    mode: ListMode::ALL,
                    receiver,
                    data: docs.into_iter().filter(|r| r.public).collect(),
                };
                if let Err(e) = sender.send(resp) {
                    error!("error sending response via channel, {}", e);
                }
            }
            Err(e) => error!(
                "error fetching local documents to answer ALL request, {}",
                e
            ),
        }
    });
}

impl NetworkBehaviourEventProcess<MdnsEvent> for DocumentBehaviour {
    /// This function is called when an event for this handler comes in
    /// There are only two events, Discovered and Expired, which are triggered when we see a
    /// new peer on the network or when an existing peer goes away.
    fn inject_event(&mut self, event: MdnsEvent) {
        match event {
            MdnsEvent::Discovered(discovered_list) => {
                for (peer, _addr) in discovered_list {
                    self.floodsub.add_node_to_partial_view(peer);
                }
            }
            MdnsEvent::Expired(expired_list) => {
                for (peer, _addr) in expired_list {
                    if !self.mdns.has_node(&peer) {
                        self.floodsub.remove_node_from_partial_view(&peer);
                    }
                }
            }
        }
    }
}

fn init_tracer() -> Result<opentelemetry::sdk::trace::Tracer, TraceError> {
    opentelemetry_jaeger::new_pipeline()
        .with_service_name("trace-demo")
        .install_batch(opentelemetry::runtime::Tokio)
}

async fn create_new_doc(name: &str, author: &str, content: &str) -> Result<(), Err> {
    let mut local_docs = read_local_docs().await?;
    let new_id = match local_docs.iter().max_by_key(|r| r.id) {
        Some(v) => v.id + 1,
        None => 0,
    };
    local_docs.push(Recipe {
        id: new_id,
        name: name.to_owned(),
        ingredients: ingredients.to_owned(),
        instructions: instructions.to_owned(),
        public: false,
    });
    write_local_docs(&local_docs).await?;

    info!("Created recipe:");
    info!("Name: {}", name);
    info!("Ingredients: {}", ingredients);
    info!("Instructions:: {}", instructions);

    Ok(())
}

async fn publish_doc(id: usize) -> Result<(), Err> {
    let mut local_docs = read_local_docs().await?;
    local_docs
        .iter_mut()
        .filter(|r| r.id == id)
        .for_each(|r| r.public = true);
    write_local_docs(&local_docs).await?;
    Ok(())
}

async fn read_local_docs() -> Result<Documents, Err> {
    let content = fs::read(STORAGE_FILE_PATH).await?;
    let result = serde_json::from_slice(&content)?;
    Ok(result)
}

async fn write_local_docs(docs: &Documents) -> Result<(), Err> {
    let json = serde_json::to_string(&docs)?;
    fs::write(STORAGE_FILE_PATH, &json).await?;
    Ok(())
}

#[tokio::main]
async fn main() {
    let peer_id = PEER_ID.clone();
    info!("Peer Id: {}", peer_id);

    let tracer = init_tracer()?;
    let span = tracer.start("node_" + peer_id);
    let cx = Context::current_with_span(span);

    /// Async channel to communicate between different parts of the application
    let (response_sender, mut response_rcv) = mpsc::unbounded_channel();

    /// Some authentication keys, which we’ll use to secure the traffic within the network
    let auth_keys = Keypair::<X25519Spec>::new()
        .into_authentic(&KEYS)
        .expect("can't create auth keys");

    /// Set TCP transport protocol that enables connection and communication between peers
    let transp = TokioTcpConfig::new()
        .upgrade(upgrade::Version::V1)
        .authenticate(NoiseConfig::xx(auth_keys).into_authenticated())
        .multiplex(mplex::MplexConfig::new())
        .boxed();

    /// Define the network behaviour (logic of the netwoek and all peers).
    /// Using floodsub protocol to deal with network events
    /// mDNS protocol for discovering other peers on the local network
    /// response_sender to propagate events back to the main part of the application.
    let mut behaviour = DocumentBehaviour {
        floodsub: Floodsub::new(PEER_ID.clone()),
        mdns: TokioMdns::new().expect("can create mdns"),
        response_sender,
    };

    /// Subscribe to the documents topic, now we can send and receive events on this topic
    behaviour.floodsub.subscribe(TOPIC.clone());

    /// Manage the created tcp connections and executes the network behaviour, triggering and
    /// receiving events and giving us a way to get to them from the outside.
    /// Uses tokio runtime to run internally
    /// Swarm contains the state og the network as a whole. (https://docs.rs/libp2p/latest/libp2p/swarm/index.html)
    let mut swarm = SwarmBuilder::new(transp, behaviour, PEER_ID.clone())
        .executor(Box::new(|fut| {
            tokio::spawn(fut);
        }))
        .build();

    /// Start the swarm
    Swarm::listen_on(
        &mut swarm,
        "/ip4/0.0.0.0/tcp/0"
            .parse()
            .expect("can get a local socket"),
    )
    .expect("swarm can be started");

    let mut stdin = tokio::io::BufReader::new(tokio::io::stdin()).lines();

    /// Event loop
    loop {
        let evt = {
            /// Wait for these three async processes, handling the first one that finishes
            tokio::select! {
                line = stdin.next_line() => Some(EventType::Input(line.expect("can get line").expect("can read line from stdin"))),
                event = swarm.next() => {
                    info!("Unhandled Swarm Event: {:?}", event);
                    None
                },
                response = response_rcv.recv() => Some(EventType::Response(response.expect("response exists"))),
            }
        };
        if let Some(event) = evt {
            match event {
                EventType::Response(resp) => {
                    let json = serde_json::to_string(&resp).expect("can jsonify response");
                    swarm.floodsub.publish(TOPIC.clone(), json.as_bytes()); // publish the response
                }
                EventType::Input(line) => match line.as_str() {
                    "ls p" => handle_list_peers(&mut swarm).await,
                    cmd if cmd.starts_with("ls d") => handle_list_docs(cmd, &mut swarm).await,
                    cmd if cmd.starts_with("create d") => handle_create_doc(cmd).await,
                    cmd if cmd.starts_with("publish.rs d") => handle_publish_doc(cmd).await,
                    _ => error!("unknown command"),
                },
            }
        }
    }
}

/// HANDLERS FOR ALL OPERATIONS

/// Discover and display all the network nodes
async fn handle_list_peers(swarm: &mut Swarm<RecipeBehaviour>) {
    info!("Discovered Peers:");
    let nodes = swarm.mdns.discovered_nodes();
    let mut unique_peers = HashSet::new();
    for peer in nodes {
        unique_peers.insert(peer);
    }
    unique_peers.iter().for_each(|p| info!("{}", p));
}

async fn handle_list_docs(cmd: &str, swarm: &mut Swarm<RecipeBehaviour>) {
    let rest = cmd.strip_prefix("ls d ");
    match rest {
        Some("all") => {
            let req = ListRequest {
                mode: ListMode::ALL,
            };
            let json = serde_json::to_string(&req).expect("can jsonify request");
            swarm.floodsub.publish(TOPIC.clone(), json.as_bytes());
        }
        Some(docs_peer_id) => {
            let req = ListRequest {
                mode: ListMode::One(docs_peer_id.to_owned()),
            };
            let json = serde_json::to_string(&req).expect("can jsonify request");
            swarm.floodsub.publish(TOPIC.clone(), json.as_bytes());
        }
        None => {
            match read_local_docs().await {
                Ok(v) => {
                    info!("Local Documents ({})", v.len());
                    v.iter().for_each(|r| info!("{:?}", r));
                }
                Err(e) => error!("error fetching local documents: {}", e),
            };
        }
    };
}

/// Crate documents throw a command
async fn handle_create_doc(cmd: &str) {
    if let Some(rest) = cmd.strip_prefix("create d") {
        let elements: Vec<&str> = rest.split("|").collect();
        if elements.len() < 3 {
            info!("too few arguments - Format: name|author|content");
        } else {
            let name = elements.get(0).expect("name is there");
            let author = elements.get(1).expect("ingredients is there");
            let content = elements.get(2).expect("instructions is there");
            if let Err(e) = create_new_doc(name, author, content).await {
                error!("error creating document: {}", e);
            };
        }
    }
}

/// Publish document throw a command
async fn handle_publish_doc(cmd: &str) {
    if let Some(rest) = cmd.strip_prefix("publish.rs d") {
        match rest.trim().parse::<usize>() {
            Ok(id) => {
                if let Err(e) = publish_doc(id).await {
                    info!("error publishing document with id {}, {}", id, e)
                } else {
                    info!("Published document with id: {}", id);
                }
            }
            Err(e) => error!("invalid id: {}, {}", rest.trim(), e),
        };
    }
}
