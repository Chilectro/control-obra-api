# main.py
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, Session
import pymysql
from fastapi import UploadFile, File
import pandas as pd
from datetime import datetime

# Configuración básica de FastAPI
app = FastAPI()

# Habilitar CORS para el frontend en React
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Ajustar a ["http://localhost:3000"] si deseas restringirlo solo a tu frontend
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuración de la base de datos
SQLALCHEMY_DATABASE_URL = "mysql+pymysql://root:RafaPedro2981@localhost/desaladora_qc"

engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

# -----------------------------
# MODELOS DE BASE DE DATOS
# -----------------------------

class Sistema(Base):
    __tablename__ = "sistemas"
    id_sistema = Column(Integer, primary_key=True, index=True)
    codigo_sistema = Column(String(20), unique=True, nullable=False)
    nombre_sistema = Column(String(100), nullable=False)

class Subsistema(Base):
    __tablename__ = "subsistemas"
    id_subsistema = Column(Integer, primary_key=True, index=True)
    id_sistema = Column(Integer, ForeignKey("sistemas.id_sistema"))
    codigo_subsistema = Column(String(20), unique=True, nullable=False)
    nombre_subsistema = Column(String(100), nullable=False)
    sistema = relationship("Sistema")

class Disciplina(Base):
    __tablename__ = "disciplinas"
    id_disciplina = Column(Integer, primary_key=True, index=True)
    nombre_disciplina = Column(String(50), nullable=False)

class Area(Base):
    __tablename__ = "areas"
    id_area = Column(Integer, primary_key=True, index=True)
    nombre_area = Column(String(100), unique=True, nullable=False)

class Protocolo(Base):
    __tablename__ = "protocolos"
    id_protocolo = Column(Integer, primary_key=True, index=True)
    id_subsistema = Column(Integer, ForeignKey("subsistemas.id_subsistema"))
    id_area = Column(Integer, ForeignKey("areas.id_area"))
    id_disciplina = Column(Integer, ForeignKey("disciplinas.id_disciplina"))
    universo = Column(Integer, nullable=False)
    aperturados = Column(Integer, nullable=False)
    cerrados = Column(Integer, nullable=False)
    abiertos = Column(Integer, nullable=False)
    aconex = Column(Integer, nullable=False)
    subsistema = relationship("Subsistema")
    area = relationship("Area")
    disciplina = relationship("Disciplina")

class PunchList(Base):
    __tablename__ = "punchlist"
    id_punchlist = Column(Integer, primary_key=True, index=True)
    id_subsistema = Column(Integer, ForeignKey("subsistemas.id_subsistema"))
    disciplina = Column(String(100))
    categoria = Column(String(50))
    fecha_compromiso = Column(String(20))
    estado = Column(String(20))
    dias_retraso = Column(Integer)
    subsistema = relationship("Subsistema")

# -----------------------------
# CREAR LAS TABLAS
# -----------------------------
Base.metadata.create_all(bind=engine)

# -----------------------------
# DEPENDENCIA PARA SESIONES
# -----------------------------
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# -----------------------------
# ENDPOINTS - SISTEMAS
# -----------------------------
@app.post("/api/sistemas")
def crear_sistema(codigo: str, nombre: str, db: Session = Depends(get_db)):
    sistema_existente = db.query(Sistema).filter(Sistema.codigo_sistema == codigo).first()
    if sistema_existente:
        raise HTTPException(status_code=400, detail="El código del sistema ya existe.")
    nuevo_sistema = Sistema(codigo_sistema=codigo, nombre_sistema=nombre)
    db.add(nuevo_sistema)
    db.commit()
    db.refresh(nuevo_sistema)
    return nuevo_sistema

@app.get("/api/sistemas")
def listar_sistemas(db: Session = Depends(get_db)):
    return db.query(Sistema).all()

@app.delete("/api/sistemas/{id_sistema}")
def eliminar_sistema(id_sistema: int, db: Session = Depends(get_db)):
    sistema = db.query(Sistema).get(id_sistema)
    if not sistema:
        raise HTTPException(status_code=404, detail="Sistema no encontrado")
    db.delete(sistema)
    db.commit()
    return {"mensaje": "Sistema eliminado"}

@app.put("/api/sistemas/{id_sistema}")
def editar_sistema(id_sistema: int, nombre: str, db: Session = Depends(get_db)):
    sistema = db.query(Sistema).get(id_sistema)
    if not sistema:
        raise HTTPException(status_code=404, detail="Sistema no encontrado")
    sistema.nombre_sistema = nombre
    db.commit()
    return sistema

# -----------------------------
# ENDPOINTS - SUBSISTEMAS
# -----------------------------
@app.post("/api/subsistemas")
def crear_subsistema(id_sistema: int, codigo: str, nombre: str, db: Session = Depends(get_db)):
    subsistema_existente = db.query(Subsistema).filter(Subsistema.codigo_subsistema == codigo).first()
    if subsistema_existente:
        raise HTTPException(status_code=400, detail="El código del subsistema ya existe.")
    nuevo_subsistema = Subsistema(
        id_sistema=id_sistema,
        codigo_subsistema=codigo,
        nombre_subsistema=nombre
    )
    db.add(nuevo_subsistema)
    db.commit()
    db.refresh(nuevo_subsistema)
    return nuevo_subsistema

@app.get("/api/subsistemas")
def listar_subsistemas(db: Session = Depends(get_db)):
    return db.query(Subsistema).all()

@app.delete("/api/subsistemas/{id_subsistema}")
def eliminar_subsistema(id_subsistema: int, db: Session = Depends(get_db)):
    subsistema = db.query(Subsistema).filter(Subsistema.id_subsistema == id_subsistema).first()
    if not subsistema:
        raise HTTPException(status_code=404, detail="Subsistema no encontrado")
    
    db.delete(subsistema)
    db.commit()
    return {"mensaje": "Subsistema eliminado correctamente"}


# -----------------------------
# ENDPOINTS - DISCIPLINAS
# -----------------------------
@app.post("/api/disciplinas")
def crear_disciplina(nombre: str, db: Session = Depends(get_db)):
    disciplina_existente = db.query(Disciplina).filter(
        Disciplina.nombre_disciplina == nombre
    ).first()
    if disciplina_existente:
        raise HTTPException(status_code=400, detail="La disciplina ya existe.")
    nueva_disciplina = Disciplina(nombre_disciplina=nombre)
    db.add(nueva_disciplina)
    db.commit()
    db.refresh(nueva_disciplina)
    return nueva_disciplina

@app.get("/api/disciplinas")
def listar_disciplinas(db: Session = Depends(get_db)):
    return db.query(Disciplina).all()

# -----------------------------
# ENDPOINTS - AREAS
# -----------------------------
@app.post("/api/areas")
def crear_area(nombre: str, db: Session = Depends(get_db)):
    area_existente = db.query(Area).filter(Area.nombre_area == nombre).first()
    if area_existente:
        raise HTTPException(status_code=400, detail="El área ya existe.")
    nueva_area = Area(nombre_area=nombre)
    db.add(nueva_area)
    db.commit()
    db.refresh(nueva_area)
    return nueva_area

@app.get("/api/areas")
def listar_areas(db: Session = Depends(get_db)):
    return db.query(Area).all()

@app.delete("/api/areas/{id_area}")
def eliminar_area(id_area: int, db: Session = Depends(get_db)):
    area = db.query(Area).get(id_area)
    if not area:
        raise HTTPException(status_code=404, detail="Área no encontrada.")
    db.delete(area)
    db.commit()
    return {"mensaje": "Área eliminada"}

# -----------------------------
# ENDPOINTS - PROTOCOLOS
# -----------------------------
@app.post("/api/protocolos")
def crear_protocolo(
    id_subsistema: int,
    id_area: int,
    id_disciplina: int,
    universo: int,
    aperturados: int,
    cerrados: int,
    abiertos: int,
    aconex: int,
    db: Session = Depends(get_db)
):
    # Validar existencia de FK
    subsistema = db.query(Subsistema).filter_by(id_subsistema=id_subsistema).first()
    area = db.query(Area).filter_by(id_area=id_area).first()
    disciplina = db.query(Disciplina).filter_by(id_disciplina=id_disciplina).first()

    if not subsistema:
        raise HTTPException(status_code=400, detail="Subsistema no encontrado.")
    if not area:
        raise HTTPException(status_code=400, detail="Área no encontrada.")
    if not disciplina:
        raise HTTPException(status_code=400, detail="Disciplina no encontrada.")

    nuevo_protocolo = Protocolo(
        id_subsistema=id_subsistema,
        id_area=id_area,
        id_disciplina=id_disciplina,
        universo=universo,
        aperturados=aperturados,
        cerrados=cerrados,
        abiertos=abiertos,
        aconex=aconex
    )
    db.add(nuevo_protocolo)
    db.commit()
    db.refresh(nuevo_protocolo)
    return nuevo_protocolo

@app.put("/api/protocolos/{id_protocolo}")
def actualizar_protocolo(
    id_protocolo: int,
    universo: int,
    aperturados: int,
    cerrados: int,
    abiertos: int,
    aconex: int,
    db: Session = Depends(get_db)
):
    protocolo = db.query(Protocolo).get(id_protocolo)
    if not protocolo:
        raise HTTPException(status_code=404, detail="Protocolo no encontrado")

    protocolo.universo = universo
    protocolo.aperturados = aperturados
    protocolo.cerrados = cerrados
    protocolo.abiertos = abiertos
    protocolo.aconex = aconex

    db.commit()
    db.refresh(protocolo)
    return protocolo

@app.get("/api/protocolos")
def listar_protocolos(db: Session = Depends(get_db)):
    return db.query(Protocolo).all()

@app.get("/api/punchlist")
def listar_punchlist(db: Session = Depends(get_db)):
    return db.query(PunchList).all()

@app.get("/api/punchlist/disciplinas")
def obtener_disciplinas_punchlist(db: Session = Depends(get_db)):
    resultados = db.query(PunchList.disciplina).distinct().order_by(PunchList.disciplina.asc()).all()
    return [r[0].strip() for r in resultados]

# Endpoint: Totales generales
@app.get("/api/punchlist/totales")
def obtener_totales_punchlist(id_subsistema: int = None, db: Session = Depends(get_db)):
    query = db.query(PunchList)
    if id_subsistema:
        query = query.filter(PunchList.id_subsistema == id_subsistema)
    total = query.count()
    abiertos = query.filter(PunchList.estado == 'Abierto').count()
    cerrados = query.filter(PunchList.estado == 'Cerrado').count()
    return {
        "total": total,
        "abiertos": abiertos,
        "cerrados": cerrados
    }

# Endpoint: Totales por categoría
@app.get("/api/punchlist/por-categoria")
def obtener_totales_por_categoria(id_subsistema: int = None, db: Session = Depends(get_db)):
    query = db.query(PunchList)
    if id_subsistema:
        query = query.filter(PunchList.id_subsistema == id_subsistema)
    resultados = (
        query.with_entities(PunchList.categoria, PunchList.estado, func.count(PunchList.id_punchlist))
        .group_by(PunchList.categoria, PunchList.estado)
        .all()
    )
    data = {}
    for categoria, estado, cantidad in resultados:
        if categoria not in data:
            data[categoria] = {"Abierto": 0, "Cerrado": 0}
        data[categoria][estado] = cantidad
    return data

# Endpoint: Totales por disciplina
@app.get("/api/punchlist/por-disciplina")
def obtener_totales_por_disciplina(id_subsistema: int = None, db: Session = Depends(get_db)):
    query = db.query(PunchList)
    if id_subsistema:
        query = query.filter(PunchList.id_subsistema == id_subsistema)
    resultados = (
        query.with_entities(PunchList.disciplina, PunchList.estado, func.count(PunchList.id_punchlist))
        .group_by(PunchList.disciplina, PunchList.estado)
        .all()
    )
    data = {}
    for disciplina, estado, cantidad in resultados:
        if disciplina not in data:
            data[disciplina] = {"Abierto": 0, "Cerrado": 0}
        data[disciplina][estado] = cantidad
    return data

# Endpoint: Porcentaje de avance
@app.get("/api/punchlist/avance")
def obtener_avance_punchlist(id_subsistema: int = None, db: Session = Depends(get_db)):
    query = db.query(PunchList)
    if id_subsistema:
        query = query.filter(PunchList.id_subsistema == id_subsistema)
    total = query.count()
    cerrados = query.filter(PunchList.estado == 'Cerrado').count()
    porcentaje = round((cerrados / total * 100), 2) if total > 0 else 0
    return {
        "total": total,
        "cerrados": cerrados,
        "porcentaje": porcentaje
    }

# Endpoint para cargar Punch List desde archivo Excel
@app.post("/api/punchlist/cargar")
def cargar_punchlist(file: UploadFile = File(...), db: Session = Depends(get_db)):
    try:
        df = pd.read_excel(file.file)
        df.columns = [col.strip() for col in df.columns]

        # 🔥 Eliminar todos los registros anteriores
        db.query(PunchList).delete()
        db.commit()

        for _, row in df.iterrows():
            subsistema = db.query(Subsistema).filter(
                Subsistema.codigo_subsistema == str(row['SUBSISTEMA']).strip()
            ).first()
            if not subsistema:
                continue

            try:
                fecha_compromiso_raw = pd.to_datetime(row['FechaCompromiso'], errors='coerce')
                if pd.isna(fecha_compromiso_raw):
                    fecha_compromiso = None
                    dias_retraso = None
                else:
                    fecha_compromiso = fecha_compromiso_raw.date()
                    dias_retraso = (datetime.today().date() - fecha_compromiso).days
            except Exception:
                fecha_compromiso = None
                dias_retraso = None

            punch = PunchList(
                id_subsistema=subsistema.id_subsistema,
                disciplina=str(row['Disciplina']).strip(),
                categoria=str(row['Categoria']).strip().split('.')[0],
                fecha_compromiso=fecha_compromiso,
                estado=str(row['Estado']).strip(),
                dias_retraso=dias_retraso
            )
            db.add(punch)

        db.commit()
        return {"mensaje": "Punch List cargado correctamente"}

    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail="Error al procesar el archivo Excel.")