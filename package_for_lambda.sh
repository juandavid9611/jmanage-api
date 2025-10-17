#!/bin/bash

# Exit if any command fails
set -eux pipefail

# --- Config ---
ZIP_NAME="lambda_function.zip"
BUILD_DIR=".lambda_build"

# Directorios de código a incluir (ajusta si agregas más)
SRC_DIRS=(
  "api"
  "builders"
  "repositories"
  "services"
  "utils"
)

# Archivos sueltos en la raíz a incluir
ROOT_PY_FILES=(
  "app.py"
  "auth.py"
  "di.py"
  "JWTBearer.py"
)

# --- Preparación ---
echo ">> Limpiando artefactos previos"
rm -f "${ZIP_NAME}" || true
rm -rf "${BUILD_DIR}"
mkdir -p "${BUILD_DIR}"

# --- Copiar código fuente ---
echo ">> Copiando código fuente"
for d in "${SRC_DIRS[@]}"; do
  if [[ -d "$d" ]]; then
    rsync -a "$d"/ "${BUILD_DIR}/${d}/" \
      --exclude "__pycache__" \
      --exclude "*.pyc" \
      --exclude ".DS_Store"
  fi
done

for f in "${ROOT_PY_FILES[@]}"; do
  if [[ -f "$f" ]]; then
    rsync -a "$f" "${BUILD_DIR}/"
  fi
done

# --- Instalar dependencias ---
# Nota: si estás en macOS/Windows y requieres wheels manylinux para Lambda,
# considera construir en Docker o usar --platform. Para flujo simple:
echo ">> Instalando dependencias en ${BUILD_DIR}"
if [[ -f "requirements.txt" ]]; then
  pip install -t "${BUILD_DIR}" -r requirements.txt
fi

# --- Empaquetar ---
echo ">> Creando zip ${ZIP_NAME}"
(
  cd "${BUILD_DIR}"
  # -x para excluir basura en el zip
  zip -r "../${ZIP_NAME}" . -x "*.DS_Store" -x "*__pycache__*" -x "*.pyc"
)

# --- Limpieza ---
echo ">> Limpiando build"
rm -rf "${BUILD_DIR}"

# --- Git (opcional) ---
if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo ">> Commit & push (opcional)"
  git add .
  git commit -m "Package Lambda: nueva estructura (api/builders/repositories/services/utils)" || true
  git push || true
fi

echo "✅ Listo: ${ZIP_NAME} creado."
