# package_for_lambda.sh
#!/bin/bash
set -euo pipefail

ZIP_NAME="lambda_function.zip"
BUILD_DIR=".lambda_build"

# Use space-separated strings (not arrays)
SRC_DIRS="api builders core repositories services utils"
ROOT_PY_FILES="app.py auth.py di.py JWTBearer.py"

IMAGE="public.ecr.aws/sam/build-python3.12:latest"
# Match your Lambda architecture: X86_64 -> linux/amd64, ARM_64 -> linux/arm64
PLATFORM="${PLATFORM:-linux/amd64}"

rm -f "${ZIP_NAME}" || true
rm -rf "${BUILD_DIR}" || true

docker run --rm \
  --platform "${PLATFORM}" \
  -v "$PWD":/workspace \
  -w /workspace \
  -e SRC_DIRS="$SRC_DIRS" \
  -e ROOT_PY_FILES="$ROOT_PY_FILES" \
  -e BUILD_DIR="$BUILD_DIR" \
  -e ZIP_NAME="$ZIP_NAME" \
  "${IMAGE}" \
  bash -lc '
    set -eux

    rm -rf "$BUILD_DIR" && mkdir -p "$BUILD_DIR"

    # 1) Install runtime deps with Linux wheels
    python -m pip install -U pip wheel setuptools
    pip install --only-binary=:all: -r requirements.txt -t "$BUILD_DIR"

    # 2) Copy selected source paths
    for d in $SRC_DIRS; do
      if [ -d "$d" ]; then
        mkdir -p "$BUILD_DIR/$d"
        rsync -a "$d"/ "$BUILD_DIR/$d"/ \
          --exclude "__pycache__" --exclude "*.pyc" --exclude ".DS_Store"
      fi
    done

    for f in $ROOT_PY_FILES; do
      if [ -f "$f" ]; then
        rsync -a "$f" "$BUILD_DIR"/
      fi
    done

    # 3) Prune caches
    find "$BUILD_DIR" -type d -name "__pycache__" -prune -exec rm -rf {} + || true
    find "$BUILD_DIR" -name "*.pyc" -delete || true

    # 4) Sanity check pydantic-core is Linux .so (optional)
    ls "$BUILD_DIR"/pydantic_core/_pydantic_core*.so >/dev/null 2>&1

    # 5) Create final zip at repo root
    (cd "$BUILD_DIR" && zip -qr "../$ZIP_NAME" .)
  '
echo "✅ Built ${ZIP_NAME} (platform=${PLATFORM})."
