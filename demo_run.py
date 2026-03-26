#!/usr/bin/env python3
"""
demo_run.py  –  End-to-end pipeline demo using stdlib sqlite3 only.
Runs all four sample fixture files through:  Extract → Transform → Validate → Load → Analyse
"""

import sqlite3
import sys
import textwrap
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

# ── Patch sqlalchemy with a stdlib-sqlite3 shim so our modules import cleanly ──
import types

def _make_sa_shim(db_conn_ref):
    """Return a minimal sqlalchemy shim backed by the provided sqlite3 connection."""

    sa = types.ModuleType("sqlalchemy")
    orm = types.ModuleType("sqlalchemy.orm")

    # ── Column / type stubs ───────────────────────────────────────────────────
    class _T:
        def __init__(self, *a, **kw): pass
        def __call__(self, *a, **kw): return self

    sa.Integer = sa.Float = sa.Text = sa.DateTime = _T()
    sa.String  = _T()

    class Column:
        _registry = {}   # (tablename, colname) → Column
        def __init__(self, *a, primary_key=False, nullable=True,
                     autoincrement=False, default=None, **kw):
            self.primary_key  = primary_key
            self.autoincrement = autoincrement
            self.nullable     = nullable
            self.default      = default
            self._attr_name   = None

        # Support SQLAlchemy column comparison expressions used in filter()
        def __ge__(self, other): return (self._attr_name, ">=", other)
        def __le__(self, other): return (self._attr_name, "<=", other)
        def __gt__(self, other): return (self._attr_name, ">",  other)
        def __lt__(self, other): return (self._attr_name, "<",  other)
        def __eq__(self, other): return (self._attr_name, "=",  other)
        def __ne__(self, other): return (self._attr_name, "!=", other)

    class ForeignKey:
        def __init__(self, *a, **kw): pass

    class relationship:
        def __init__(self, *a, **kw): pass

    sa.Column      = Column
    sa.ForeignKey  = ForeignKey
    orm.relationship = relationship

    # ── DeclarativeBase ───────────────────────────────────────────────────────
    _model_registry = {}   # tablename → class

    class _Meta(type):
        def __new__(mcs, name, bases, ns):
            cls = super().__new__(mcs, name, bases, ns)
            tn  = ns.get("__tablename__")
            if tn:
                cls._pk_col  = None
                cls._columns = {}
                for attr, val in ns.items():
                    if isinstance(val, Column):
                        val._attr_name = attr
                        cls._columns[attr] = val
                        if val.primary_key:
                            cls._pk_col = attr
                _model_registry[tn] = cls

                # Auto-generate __init__ so ORM(col=val, ...) works
                col_names = list(cls._columns.keys())
                def _init(self, **kwargs):
                    for c in col_names:
                        setattr(self, c, kwargs.get(c))
                cls.__init__ = _init

            return cls

    class DeclarativeBase(metaclass=_Meta):
        pass

    class _Metadata:
        def create_all(self, engine):
            _TYPE = {
                "integer": "INTEGER", "float": "REAL", "real": "REAL",
                "text": "TEXT", "datetime": "TEXT", "varchar": "TEXT",
            }
            for tn, cls in _model_registry.items():
                cols_sql = []
                for cname, col in cls._columns.items():
                    t = "TEXT"
                    if col.primary_key:
                        cols_sql.append(f'"{cname}" INTEGER PRIMARY KEY AUTOINCREMENT')
                        continue
                    cols_sql.append(f'"{cname}" {t}')
                ddl = f'CREATE TABLE IF NOT EXISTS "{tn}" ({", ".join(cols_sql)})'
                db_conn_ref[0].execute(ddl)
            db_conn_ref[0].commit()

    class Base(DeclarativeBase):
        metadata = _Metadata()

    orm.DeclarativeBase = Base

    # ── Session ───────────────────────────────────────────────────────────────
    class Session:
        def __init__(self):
            self._pending = []

        def add(self, obj):
            self._pending.append(obj)

        def bulk_save_objects(self, objs):
            self._pending.extend(objs)

        def flush(self):
            self._commit_pending()

        def commit(self):
            self._commit_pending()
            db_conn_ref[0].commit()

        def rollback(self):
            self._pending.clear()
            try: db_conn_ref[0].rollback()
            except Exception: pass

        def close(self): pass

        def _commit_pending(self):
            for obj in self._pending:
                tn   = obj.__tablename__
                cls  = type(obj)
                pk   = cls._pk_col
                cols = [c for c in cls._columns if c != pk]
                vals = []
                for c in cols:
                    v = getattr(obj, c, None)
                    if v is None and cls._columns[c].default:
                        d = cls._columns[c].default
                        v = d() if callable(d) else d
                    if isinstance(v, datetime):
                        v = v.isoformat()
                    vals.append(v)
                ph  = ",".join("?" * len(cols))
                cn  = ",".join(f'"{c}"' for c in cols)
                sql = f'INSERT INTO "{tn}" ({cn}) VALUES ({ph})'
                cur = db_conn_ref[0].execute(sql, vals)
                setattr(obj, pk, cur.lastrowid)
            self._pending.clear()

        def query(self, *entities):
            return _QB(entities)

    class _QB:
        def __init__(self, entities):
            self._e       = entities
            self._wheres  = []
            self._orderby = []
            self._lim     = None

        def filter(self, *conds):
            self._wheres.extend(conds)
            return self

        def join(self, *a): return self

        def order_by(self, *cols):
            self._orderby.extend(cols)
            return self

        def limit(self, n):
            self._lim = n
            return self

        def all(self):
            cls = self._e[0]
            if not hasattr(cls, "__tablename__"):
                return []
            tn  = cls.__tablename__
            sql = f'SELECT * FROM "{tn}"'
            # Apply simple equality filters stored as tuples
            params = []
            where_parts = []
            for cond in self._wheres:
                if isinstance(cond, tuple) and len(cond) == 3:
                    col, op, val = cond
                    where_parts.append(f'"{col}" {op} ?')
                    # Normalise timestamps to ISO strings for TEXT storage
                    if hasattr(val, 'isoformat'):
                        val = val.isoformat()
                    elif hasattr(val, 'to_pydatetime'):
                        val = val.to_pydatetime().isoformat()
                    params.append(val)
            if where_parts:
                sql += " WHERE " + " AND ".join(where_parts)
            for ocol in self._orderby:
                if isinstance(ocol, tuple) and len(ocol) == 2:
                    sql += f' ORDER BY "{ocol[0]}" {ocol[1]}'
            if self._lim:
                sql += f" LIMIT {self._lim}"
            rows = db_conn_ref[0].execute(sql, params).fetchall()
            return [_row_to(cls, r) for r in rows]

        def first(self):
            r = self.all()
            return r[0] if r else None

        def count(self):
            cls = self._e[0]
            if not hasattr(cls, "__tablename__"):
                return 0
            tn  = cls.__tablename__
            return db_conn_ref[0].execute(f'SELECT COUNT(*) FROM "{tn}"').fetchone()[0]

    def _row_to(cls, row):
        obj = object.__new__(cls)
        for i, col in enumerate(db_conn_ref[0].execute(
                f'SELECT * FROM "{cls.__tablename__}" LIMIT 0').description):
            setattr(obj, col[0], row[i] if not isinstance(row, sqlite3.Row) else row[col[0]])
        return obj

    # ── patched describe workaround: always use row_factory ──────────────────
    def _row_to(cls, row):
        obj = object.__new__(cls)
        keys = [d[0] for d in db_conn_ref[0].execute(
            f'SELECT * FROM "{cls.__tablename__}" LIMIT 0').description]
        for i, k in enumerate(keys):
            setattr(obj, k, row[i])
        return obj

    sa.create_engine  = lambda *a, **kw: None
    sa.text           = lambda s: s
    sa.event          = types.SimpleNamespace(
        listens_for=lambda *a, **kw: (lambda f: f)
    )
    orm.sessionmaker  = lambda bind=None: Session
    orm.Session       = Session

    # ── Wire get_session / init_db to use OUR connection ─────────────────────
    def patched_init_db(db_path):
        if db_path == ":memory:" or db_path == "data/calibration.db":
            if db_conn_ref[0] is None:
                db_conn_ref[0] = sqlite3.connect(":memory:", check_same_thread=False)
        else:
            db_conn_ref[0] = sqlite3.connect(db_path, check_same_thread=False)
        Base.metadata.create_all(None)   # create_all ignores engine arg
        return None  # engine not used

    def patched_get_session(engine):
        return Session()

    return sa, orm, patched_init_db, patched_get_session, Base


# ── Bootstrap ─────────────────────────────────────────────────────────────────
_conn_ref = [sqlite3.connect(":memory:", check_same_thread=False)]

sa_mod, orm_mod, _init_db, _get_session, _Base = _make_sa_shim(_conn_ref)
sys.modules["sqlalchemy"]          = sa_mod
sys.modules["sqlalchemy.orm"]      = orm_mod

# Now import our project modules (they'll see the shim)
from src.database.schema import (
    CalibrationRun, CalibrationMeasurement, FileImport, ValidationResult
)

# Manually create tables using the real schema objects + our shim
from src.database import schema as _schema
_schema.Base.metadata.create_all(None)

# Patch init_db / get_session at runtime
import src.database.schema as _schema_mod
_schema_mod.init_db     = lambda db_path: None
_schema_mod.get_session = lambda engine: (_get_session(None))

from src.etl.extractors    import ExtractorFactory
from src.etl.transformers  import transform
from src.etl.loaders       import load_to_database, check_already_imported
from src.validation.validators import run_all_validations, format_validation_report


# ── Helpers ────────────────────────────────────────────────────────────────────
SEP = "=" * 65

def section(title):
    print(f"\n{SEP}\n  {title}\n{SEP}")


# ── Main run ───────────────────────────────────────────────────────────────────
def main():
    session = _get_session(None)

    fixtures = Path("tests/fixtures")
    files    = sorted(fixtures.glob("sample_calibration.*"))

    print(f"\n{'='*65}")
    print("  CALIBRATION PIPELINE – END-TO-END DEMO RUN")
    print(f"  {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*65}")
    print(f"  Files to process: {[f.name for f in files]}\n")

    total_rows = 0
    run_count  = 0

    for filepath in files:
        section(f"FILE: {filepath.name}")

        # 1 ── Extract
        try:
            df, file_hash = ExtractorFactory.extract(str(filepath))
            print(f"  ✓ Extracted  {len(df)} rows  |  cols: {list(df.columns)}")
        except Exception as e:
            print(f"  ✗ Extract failed: {e}")
            continue

        # 2 ── Dedup
        if check_already_imported(session, file_hash):
            print(f"  ⟳ Skipped (already imported, hash match)")
            continue

        # 3 ── Transform
        df = transform(df, source_file=filepath.name)
        print(f"  ✓ Transformed {len(df)} rows  |  deviation range: "
              f"[{df['deviation'].min():.4f}, {df['deviation'].max():.4f}]")

        # 4 ── Validate
        results = run_all_validations(df, session=session)
        statuses = {r["status"] for r in results}
        fail_count = sum(1 for r in results if r["status"] == "fail")
        warn_count = sum(1 for r in results if r["status"] == "warn")
        pass_count = sum(1 for r in results if r["status"] == "pass")
        print(f"  ✓ Validated   {len(results)} checks  |  "
              f"pass={pass_count}  warn={warn_count}  fail={fail_count}")
        if warn_count or fail_count:
            for r in results:
                if r["status"] in ("warn", "fail"):
                    icon = "⚠" if r["status"] == "warn" else "✗"
                    print(f"      {icon} [{r['check_type']}] {r['details']}")

        # 5 ── Load
        load_result = load_to_database(
            df, results, session,
            source_file=filepath.name,
            file_hash=file_hash,
        )
        if load_result["status"] == "success":
            print(f"  ✓ Loaded      {load_result['rows_loaded']} measurements  "
                  f"(run_id={load_result['run_id']})")
            total_rows += load_result["rows_loaded"]
            run_count  += 1
        else:
            print(f"  ✗ Load failed: {load_result['message']}")

    # ── Summary ────────────────────────────────────────────────────────────────
    section("PIPELINE SUMMARY")
    print(f"  Files processed : {run_count} / {len(files)}")
    print(f"  Rows loaded     : {total_rows}")

    # ── Quick trend peek ───────────────────────────────────────────────────────
    section("TREND SNAPSHOT (from loaded data)")

    rows = _conn_ref[0].execute(
        'SELECT equipment_id, COUNT(*) as n, AVG(deviation) as mean_dev, '
        'MIN(deviation) as min_dev, MAX(deviation) as max_dev '
        'FROM calibration_measurements cm '
        'JOIN calibration_runs cr ON cm.run_id = cr.run_id '
        'GROUP BY equipment_id'
    ).fetchall()

    if rows:
        print(f"\n  {'Equipment':<12}  {'N':>5}  {'Mean dev':>10}  {'Min':>10}  {'Max':>10}")
        print(f"  {'-'*12}  {'-'*5}  {'-'*10}  {'-'*10}  {'-'*10}")
        for r in rows:
            eq, n, mean, lo, hi = r
            mean, lo, hi = float(mean), float(lo), float(hi)
            alert = "  ⚠ ALERT" if abs(mean) > 0.5 else ""
            print(f"  {str(eq):<12}  {n:>5}  {mean:>10.4f}  {lo:>10.4f}  {hi:>10.4f}{alert}")
    else:
        print("  (no data)")

    pos_rows = _conn_ref[0].execute(
        'SELECT position, AVG(ABS(deviation)) as mad '
        'FROM calibration_measurements GROUP BY position '
        'ORDER BY mad DESC LIMIT 5'
    ).fetchall()

    print(f"\n  Top 5 worst positions by mean absolute deviation:")
    print(f"  {'Position':<12}  {'Mean |dev|':>12}")
    print(f"  {'-'*12}  {'-'*12}")
    for pos, mad in pos_rows:
        print(f"  {str(pos):<12}  {mad:>12.4f}")

    section("DONE")
    print(f"  Database: in-memory SQLite  |  Runs: {run_count}  |  Measurements: {total_rows}\n")


if __name__ == "__main__":
    main()
