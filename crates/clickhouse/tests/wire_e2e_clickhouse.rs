//! End-to-end tests against an isolated ClickHouse server container.

use std::{
    net::{TcpStream, ToSocketAddrs},
    process::Command,
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use clickhouse::{
    ArrayColumn, ChBool, Date, Date32, DateTime, DateTime64, Decimal32, Decimal64, Decimal128,
    Enum8, Enum16, FixedKind, FixedStringColumn, GeoColumn, GeoKind, Ipv4, Ipv6,
    LowCardinalityColumn, MapColumn, NativeClient, NullableColumn, StringColumn, Time, Time64,
    TupleColumn, Uuid, WireBlock, WireColumn,
};
use zerocopy::FromBytes;

struct ClickHouseContainer {
    name: String,
    native_port: u16,
}

impl ClickHouseContainer {
    fn start() -> Self {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let name = format!("chcxx-t02-{}-{stamp}", std::process::id());
        let output = Command::new("docker")
            .args([
                "run",
                "--rm",
                "-d",
                "--name",
                &name,
                "-p",
                "0:9000",
                "-p",
                "0:8123",
                "-e",
                "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1",
                "clickhouse/clickhouse-server:26.8.2.7",
            ])
            .output()
            .expect("run docker");
        assert!(
            output.status.success(),
            "docker run failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let native_port = published_port(&name, "9000/tcp");
        let container = Self { name, native_port };
        container.wait_ready();
        container
    }

    fn connect(&self) -> NativeClient {
        NativeClient::connect("127.0.0.1", self.native_port).expect("connect")
    }

    fn wait_ready(&self) {
        for _ in 0..120 {
            let address = format!("127.0.0.1:{}", self.native_port);
            if let Ok(mut addresses) = address.to_socket_addrs()
                && let Some(address) = addresses.next()
                && TcpStream::connect_timeout(&address, Duration::from_millis(250)).is_ok()
                && let Ok(mut client) = NativeClient::connect("127.0.0.1", self.native_port)
                && client.ping().is_ok()
            {
                return;
            }
            thread::sleep(Duration::from_secs(1));
        }
        panic!(
            "ClickHouse did not become ready on port {}",
            self.native_port
        );
    }
}

impl Drop for ClickHouseContainer {
    fn drop(&mut self) {
        let _cleanup = Command::new("docker")
            .args(["rm", "-f", &self.name])
            .output();
    }
}

fn published_port(name: &str, container_port: &str) -> u16 {
    for _ in 0..60 {
        let output = Command::new("docker")
            .args(["port", name, container_port])
            .output()
            .expect("docker port");
        if output.status.success() {
            let text = String::from_utf8_lossy(&output.stdout);
            if let Some(port) = text
                .lines()
                .find_map(|line| line.rsplit(':').next().and_then(|port| port.parse().ok()))
            {
                return port;
            }
        }
        thread::sleep(Duration::from_millis(250));
    }
    panic!("no published port for {name} {container_port}");
}

#[test]
fn native_round_trip_primitives() {
    let container = ClickHouseContainer::start();
    let mut client = container.connect();
    client.ping().expect("ping");
    client
        .execute("DROP TABLE IF EXISTS t02_simple")
        .expect("drop");
    client
        .execute("CREATE TABLE t02_simple (id UInt32, name String) ENGINE = Memory")
        .expect("create");

    let mut block = WireBlock::new().expect("block");
    block
        .append("id", WireColumn::fixed(&[1_u32, 2]).expect("id"))
        .expect("append id");
    block
        .append(
            "name",
            WireColumn::strings(["alpha", "beta"]).expect("name"),
        )
        .expect("append name");
    client.insert("t02_simple", &block).expect("insert");

    let selected = client
        .select_one("SELECT id, name FROM t02_simple ORDER BY id")
        .expect("select")
        .expect("block");
    let handle = selected.column_handle("id").expect("id handle");
    let bytes = handle.as_ref().expect("id column").fixed_bytes();
    eprintln!(
        "id bytes len={} ptr={:p} mod4={}",
        bytes.len(),
        bytes.as_ptr(),
        bytes.as_ptr() as usize % 4
    );
    assert_eq!(
        selected.column_to_vec::<u32>("id").expect("ids"),
        vec![1, 2]
    );
    assert_eq!(
        selected.column_slice::<u32>("id").expect("ids slice"),
        &[1, 2]
    );
    assert_eq!(selected.column_str("name", 0).expect("name0"), "alpha");
    assert_eq!(selected.column_str("name", 1).expect("name1"), "beta");
}

#[test]
fn native_round_trip_full_type_matrix() {
    let container = ClickHouseContainer::start();
    let mut client = container.connect();
    client.ping().expect("ping");
    client
        .execute("SET allow_experimental_json_type = 1")
        .expect("json setting");
    client
        .execute("SET output_format_native_write_json_as_string = 1")
        .expect("json output setting");
    client
        .execute("DROP TABLE IF EXISTS t02_all_types")
        .expect("drop");
    client
        .execute(
            "CREATE TABLE t02_all_types (u8 UInt8, u16 UInt16, u32 UInt32, u64 UInt64, u128 \
             UInt128, i8 Int8, i16 Int16, i32 Int32, i64 Int64, i128 Int128, f32 Float32, f64 \
             Float64, b Bool, d Date, d32 Date32, dt DateTime, dt64 DateTime64(3), tm Time, tm64 \
             Time64(3), dec32 Decimal32(2), dec64 Decimal64(4), dec128 Decimal128(8), uuid UUID, \
             ip4 IPv4, ip6 IPv6, fs FixedString(4), e8 Enum8('a' = 1, 'b' = 2), e16 Enum16('a' = \
             100, 'b' = 200), s String, js JSON, n Nullable(UInt32), arr Array(UInt32), lc \
             LowCardinality(String), tup Tuple(UInt32, String), m Map(String, UInt64), pt Point) \
             ENGINE = Memory",
        )
        .expect("create");

    let mut block = WireBlock::new().expect("block");
    let fixed = |name: &str, column: WireColumn, block: &mut WireBlock| {
        block.append(name, column).expect(name);
    };
    fixed("u8", WireColumn::fixed(&[1_u8]).expect("u8"), &mut block);
    fixed("u16", WireColumn::fixed(&[2_u16]).expect("u16"), &mut block);
    fixed("u32", WireColumn::fixed(&[3_u32]).expect("u32"), &mut block);
    fixed("u64", WireColumn::fixed(&[4_u64]).expect("u64"), &mut block);
    fixed(
        "u128",
        WireColumn::fixed(&[5_u128]).expect("u128"),
        &mut block,
    );
    fixed("i8", WireColumn::fixed(&[-1_i8]).expect("i8"), &mut block);
    fixed(
        "i16",
        WireColumn::fixed(&[-2_i16]).expect("i16"),
        &mut block,
    );
    fixed(
        "i32",
        WireColumn::fixed(&[-3_i32]).expect("i32"),
        &mut block,
    );
    fixed(
        "i64",
        WireColumn::fixed(&[-4_i64]).expect("i64"),
        &mut block,
    );
    fixed(
        "i128",
        WireColumn::fixed(&[-5_i128]).expect("i128"),
        &mut block,
    );
    fixed(
        "f32",
        WireColumn::fixed(&[1.5_f32]).expect("f32"),
        &mut block,
    );
    fixed(
        "f64",
        WireColumn::fixed(&[-2.5_f64]).expect("f64"),
        &mut block,
    );
    fixed(
        "b",
        WireColumn::fixed(&[ChBool(1)]).expect("bool"),
        &mut block,
    );
    fixed(
        "d",
        WireColumn::fixed(&[Date(1)]).expect("date"),
        &mut block,
    );
    fixed(
        "d32",
        WireColumn::fixed(&[Date32(-1)]).expect("date32"),
        &mut block,
    );
    fixed(
        "dt",
        WireColumn::fixed(&[DateTime(1)]).expect("datetime"),
        &mut block,
    );
    fixed(
        "dt64",
        WireColumn::fixed(&[DateTime64(1_000)]).expect("datetime64"),
        &mut block,
    );
    fixed(
        "tm",
        WireColumn::fixed(&[Time(1)]).expect("time"),
        &mut block,
    );
    fixed(
        "tm64",
        WireColumn::fixed(&[Time64(1_000)]).expect("time64"),
        &mut block,
    );
    fixed(
        "dec32",
        WireColumn::fixed(&[Decimal32(123)]).expect("decimal32"),
        &mut block,
    );
    fixed(
        "dec64",
        WireColumn::fixed(&[Decimal64(1_234)]).expect("decimal64"),
        &mut block,
    );
    fixed(
        "dec128",
        WireColumn::fixed(&[Decimal128(123_456)]).expect("decimal128"),
        &mut block,
    );
    fixed(
        "uuid",
        WireColumn::fixed(&[Uuid([1; 16])]).expect("uuid"),
        &mut block,
    );
    fixed(
        "ip4",
        WireColumn::fixed(&[Ipv4(1)]).expect("ipv4"),
        &mut block,
    );
    fixed(
        "ip6",
        WireColumn::fixed(&[Ipv6([1; 16])]).expect("ipv6"),
        &mut block,
    );
    let mut fs = FixedStringColumn::new(4).expect("fixed string");
    fs.append(b"ab").expect("fixed string value");
    block.append("fs", WireColumn::FixedString(fs)).expect("fs");
    fixed(
        "e8",
        WireColumn::fixed_named(FixedKind::Enum8, "Enum8('a' = 1, 'b' = 2)", &[Enum8(1)])
            .expect("enum8"),
        &mut block,
    );
    fixed(
        "e16",
        WireColumn::fixed_named(
            FixedKind::Enum16,
            "Enum16('a' = 100, 'b' = 200)",
            &[Enum16(100)],
        )
        .expect("enum16"),
        &mut block,
    );
    block
        .append("s", WireColumn::strings(["hello"]).expect("string"))
        .expect("s");
    let mut json = StringColumn::new_json().expect("json");
    json.append_str("{\"a\":1}").expect("json value");
    block.append("js", WireColumn::Json(json)).expect("js");
    let inner = WireColumn::fixed(&[0_u32]).expect("nullable inner");
    let mut nullable = NullableColumn::new(&inner).expect("nullable");
    nullable.append_fixed(&[42_u32]).expect("nullable value");
    block
        .append("n", WireColumn::Nullable(Box::new(nullable)))
        .expect("n");
    let mut array = ArrayColumn::new("UInt32").expect("array");
    let values = WireColumn::fixed(&[1_u32, 2]).expect("array values");
    array.append_row(&values).expect("array row");
    block
        .append("arr", WireColumn::Array(Box::new(array)))
        .expect("arr");
    let mut low_cardinality = LowCardinalityColumn::new("String").expect("low cardinality");
    low_cardinality
        .append_str("x")
        .expect("low cardinality value");
    block
        .append("lc", WireColumn::LowCardinality(Box::new(low_cardinality)))
        .expect("lc");
    let mut tuple = TupleColumn::new(&["UInt32", "String"]).expect("tuple");
    tuple
        .append_fixed(0, &7_u32.to_le_bytes())
        .expect("tuple fixed");
    tuple.append_str(1, "t").expect("tuple string");
    block.append("tup", WireColumn::Tuple(tuple)).expect("tup");
    let mut map = MapColumn::new("String", "UInt64").expect("map");
    let keys = WireColumn::strings(["k"]).expect("map keys");
    let values = WireColumn::fixed(&[9_u64]).expect("map values");
    map.append_row(&keys, &values).expect("map row");
    block.append("m", WireColumn::Map(map)).expect("m");
    let mut point = GeoColumn::new(GeoKind::Point).expect("point");
    point.append_point(1.5, 2.5).expect("point value");
    block.append("pt", WireColumn::Geo(point)).expect("pt");

    client.insert("t02_all_types", &block).expect("insert");
    let selected = client
        .select_one("SELECT * FROM t02_all_types")
        .expect("select")
        .expect("block");
    assert_eq!(selected.row_count(), 1);
    assert_eq!(selected.column_slice::<u8>("u8").expect("u8"), &[1]);
    assert_eq!(selected.column_slice::<u16>("u16").expect("u16"), &[2]);
    assert_eq!(selected.column_slice::<u32>("u32").expect("u32"), &[3]);
    assert_eq!(selected.column_slice::<u64>("u64").expect("u64"), &[4]);
    assert_eq!(selected.column_slice::<u128>("u128").expect("u128"), &[5]);
    assert_eq!(selected.column_slice::<i8>("i8").expect("i8"), &[-1]);
    assert_eq!(selected.column_slice::<i16>("i16").expect("i16"), &[-2]);
    assert_eq!(selected.column_slice::<i32>("i32").expect("i32"), &[-3]);
    assert_eq!(selected.column_slice::<i64>("i64").expect("i64"), &[-4]);
    assert_eq!(selected.column_slice::<i128>("i128").expect("i128"), &[-5]);
    assert_eq!(selected.column_slice::<f32>("f32").expect("f32"), &[1.5]);
    assert_eq!(selected.column_slice::<f64>("f64").expect("f64"), &[-2.5]);
    assert_eq!(
        selected.column_slice::<ChBool>("b").expect("bool"),
        &[ChBool(1)]
    );
    assert_eq!(
        selected.column_to_vec::<Date>("d").expect("date"),
        vec![Date(1)]
    );
    assert_eq!(
        selected.column_to_vec::<Date32>("d32").expect("date32"),
        vec![Date32(-1)]
    );
    assert_eq!(
        selected.column_to_vec::<DateTime>("dt").expect("datetime"),
        vec![DateTime(1)]
    );
    assert_eq!(
        selected
            .column_to_vec::<DateTime64>("dt64")
            .expect("datetime64"),
        vec![DateTime64(1_000)]
    );
    assert_eq!(
        selected.column_to_vec::<Time>("tm").expect("time"),
        vec![Time(1)]
    );
    assert_eq!(
        selected.column_to_vec::<Time64>("tm64").expect("time64"),
        vec![Time64(1_000)]
    );
    assert_eq!(
        selected
            .column_to_vec::<Decimal32>("dec32")
            .expect("decimal32"),
        vec![Decimal32(123)]
    );
    assert_eq!(
        selected
            .column_to_vec::<Decimal64>("dec64")
            .expect("decimal64"),
        vec![Decimal64(1_234)]
    );
    assert_eq!(
        selected
            .column_to_vec::<Decimal128>("dec128")
            .expect("decimal128"),
        vec![Decimal128(123_456)]
    );
    assert_eq!(
        selected.column_to_vec::<Uuid>("uuid").expect("uuid"),
        vec![Uuid([1; 16])]
    );
    assert_eq!(
        selected.column_to_vec::<Ipv4>("ip4").expect("ipv4"),
        vec![Ipv4(1)]
    );
    assert_eq!(
        selected.column_to_vec::<Ipv6>("ip6").expect("ipv6"),
        vec![Ipv6([1; 16])]
    );
    assert_eq!(selected.column_bytes("fs", 0).expect("fs"), b"ab\0\0");
    assert_eq!(
        selected.column_to_vec::<Enum8>("e8").expect("enum8"),
        vec![Enum8(1)]
    );
    assert_eq!(
        selected.column_to_vec::<Enum16>("e16").expect("enum16"),
        vec![Enum16(100)]
    );
    assert_eq!(selected.column_str("s", 0).expect("s"), "hello");
    let json = selected.column_str("js", 0).expect("js");
    assert!(json.contains("\"a\"") && json.contains('1'));
    assert_eq!(selected.column_nulls("n").expect("nulls"), &[0]);
    let handle = selected.column_handle("n").expect("nullable handle");
    let nested = handle.as_ref().expect("handle").nested();
    let nested = nested.as_ref().expect("nested");
    let values = <[u32]>::ref_from_bytes(nested.fixed_bytes()).expect("nullable values");
    assert_eq!(values, &[42]);
    assert_eq!(selected.column_offsets("arr").expect("offsets"), &[2]);
    let handle = selected.column_handle("arr").expect("array handle");
    let values = handle.as_ref().expect("handle").nested();
    let values = values.as_ref().expect("array values");
    let values = <[u32]>::ref_from_bytes(values.fixed_bytes()).expect("array values");
    assert_eq!(values, &[1, 2]);
    assert_eq!(selected.column_str("lc", 0).expect("lc"), "x");
    let handle = selected.column_handle("tup").expect("tuple handle");
    let handle = handle.as_ref().expect("handle");
    let left = handle.tuple_at(0);
    let right = handle.tuple_at(1);
    assert_eq!(
        <[u32]>::ref_from_bytes(left.as_ref().expect("left").fixed_bytes()).expect("left"),
        &[7]
    );
    assert_eq!(right.as_ref().expect("right").string_at(0), b"t");
    assert!(
        selected
            .column_type_name("m")
            .expect("map type")
            .starts_with("Map(")
    );
    assert_eq!(
        selected.column_type_name("pt").expect("point type"),
        "Point"
    );
}

#[test]
fn native_round_trip_geo_and_low_cardinality_numeric() {
    let container = ClickHouseContainer::start();
    let mut client = container.connect();
    client
        .execute("SET allow_suspicious_low_cardinality_types = 1")
        .expect("low cardinality setting");
    client
        .execute("DROP TABLE IF EXISTS t02_geo")
        .expect("drop");
    client
        .execute(
            "CREATE TABLE t02_geo (ring Ring, poly Polygon, mp MultiPolygon, lcu \
             LowCardinality(UInt64)) ENGINE = Memory",
        )
        .expect("create");

    let mut point = TupleColumn::new(&["Float64", "Float64"]).expect("point tuple");
    point
        .append_fixed(0, &1.0_f64.to_le_bytes())
        .expect("point x");
    point
        .append_fixed(1, &2.0_f64.to_le_bytes())
        .expect("point y");
    let point = WireColumn::Tuple(point);

    let mut ring = ArrayColumn::new("Tuple(Float64, Float64)").expect("ring");
    ring.append_row(&point).expect("ring row");
    let ring_column = WireColumn::Array(Box::new(ring));

    let mut polygon = ArrayColumn::new("Array(Tuple(Float64, Float64))").expect("polygon");
    polygon.append_row(&ring_column).expect("polygon row");
    let polygon_column = WireColumn::Array(Box::new(polygon));

    let mut multi =
        ArrayColumn::new("Array(Array(Tuple(Float64, Float64)))").expect("multipolygon");
    multi.append_row(&polygon_column).expect("multipolygon row");

    let mut low_cardinality = LowCardinalityColumn::new("UInt64").expect("low cardinality");
    low_cardinality
        .append_u64(11)
        .expect("low cardinality value");

    let mut block = WireBlock::new().expect("block");
    block.append("ring", ring_column).expect("ring");
    block.append("poly", polygon_column).expect("poly");
    block
        .append("mp", WireColumn::Array(Box::new(multi)))
        .expect("mp");
    block
        .append("lcu", WireColumn::LowCardinality(Box::new(low_cardinality)))
        .expect("lcu");
    client.insert("t02_geo", &block).expect("insert");

    let selected = client
        .select_one("SELECT * FROM t02_geo")
        .expect("select")
        .expect("block");
    assert_eq!(selected.row_count(), 1);
    assert_eq!(
        selected.column_type_name("ring").expect("ring type"),
        "Ring"
    );
    assert_eq!(
        selected.column_type_name("poly").expect("poly type"),
        "Polygon"
    );
    assert_eq!(
        selected.column_type_name("mp").expect("mp type"),
        "MultiPolygon"
    );
    let handle = selected.column_handle("lcu").expect("lcu handle");
    let bytes = handle.as_ref().expect("lcu").item_bytes(0);
    assert_eq!(u64::from_le_bytes(bytes.try_into().expect("u64")), 11);
}
