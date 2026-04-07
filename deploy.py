"""
deploy.py — Build and push the MLB HR Props dashboard to GitHub Pages.

Usage:
  python deploy.py              # run pipeline then deploy
  python deploy.py --skip-pipeline  # deploy existing data.json only
  python deploy.py --dry-run   # build only, no git push

Mirrors the NBA pipeline deploy.py structure.
Repo expected at C:\\mlb-props (Windows) or ~/mlb-props (Mac/Linux).
"""

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

# ── Config — edit these ───────────────────────────────────────────────────────
GITHUB_REPO   = "https://github.com/dm-2026/mlb-props.git"   # your repo URL
DEPLOY_BRANCH = "gh-pages"

# Files and folders to include in the deploy
DEPLOY_INCLUDES = [
    "index.html",
    "data/data.json",
    "og-image.png",
]

BUILD_DIR = Path("build")


def run_cmd(cmd, check=True, cwd=None):
    print(f"  $ {' '.join(cmd)}")
    result = subprocess.run(cmd, check=check, cwd=cwd, capture_output=False)
    return result


def build():
    """Copy deploy files into build/ directory."""
    print("\n[1/3] Building…")
    if BUILD_DIR.exists():
        shutil.rmtree(BUILD_DIR)
    BUILD_DIR.mkdir()

    for item in DEPLOY_INCLUDES:
        src = Path(item)
        dst = BUILD_DIR / src
        dst.parent.mkdir(parents=True, exist_ok=True)
        if src.exists():
            shutil.copy2(src, dst)
            print(f"  Copied {src} → {dst}")
        else:
            print(f"  [WARNING] {src} not found — skipping")

    # Write a .nojekyll so GitHub Pages serves data.json correctly
    (BUILD_DIR / ".nojekyll").touch()
    print("  Created .nojekyll")


def deploy(dry_run=False):
    """Push build/ to gh-pages branch."""
    print("\n[2/3] Deploying to GitHub Pages…")

    if dry_run:
        print("  --dry-run: skipping git push")
        print(f"  Build ready in: {BUILD_DIR.resolve()}")
        return

    # Check git available
    try:
        run_cmd(["git", "--version"], check=True)
    except FileNotFoundError:
        print("  ERROR: git not found. Install git and try again.")
        sys.exit(1)

    # Ensure we're in a git repo
    git_dir = Path(".git")
    if not git_dir.exists():
        print("  Initializing git repo…")
        run_cmd(["git", "init"])
        run_cmd(["git", "remote", "add", "origin", GITHUB_REPO])

    # Check remote exists
    result = subprocess.run(["git", "remote", "-v"], capture_output=True, text=True)
    if "origin" not in result.stdout:
        run_cmd(["git", "remote", "add", "origin", GITHUB_REPO])

    # Use git worktree or subtree push approach
    # Simplest: commit build/ content to gh-pages
    run_cmd(["git", "add", "-A"])
    try:
        run_cmd(["git", "commit", "-m", "chore: update MLB HR props data"])
    except subprocess.CalledProcessError:
        print("  Nothing to commit on main branch — continuing")

    # Push build/ as gh-pages subtree
    try:
        run_cmd([
            "git", "subtree", "push",
            "--prefix", str(BUILD_DIR),
            "origin", DEPLOY_BRANCH
        ])
        print(f"\n  ✓ Deployed to {DEPLOY_BRANCH} branch")
        print(f"  Dashboard: https://dm-2026.github.io/mlb-props/")
    except subprocess.CalledProcessError:
        print("\n  subtree push failed. Trying force push via orphan branch…")
        # Alternative: create orphan commit from build/
        worktree_path = Path("_gh_pages_tmp")
        try:
            if worktree_path.exists():
                shutil.rmtree(worktree_path)
            run_cmd(["git", "worktree", "add", "--orphan", str(worktree_path), DEPLOY_BRANCH])
            # Copy build contents into worktree
            for f in BUILD_DIR.rglob("*"):
                if f.is_file():
                    rel = f.relative_to(BUILD_DIR)
                    dst = worktree_path / rel
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(f, dst)
            run_cmd(["git", "add", "-A"], cwd=worktree_path)
            run_cmd(["git", "commit", "-m", "deploy: MLB HR props update"], cwd=worktree_path)
            run_cmd(["git", "push", "origin", DEPLOY_BRANCH, "--force"], cwd=worktree_path)
            print(f"\n  ✓ Force-deployed to {DEPLOY_BRANCH}")
        finally:
            run_cmd(["git", "worktree", "remove", "--force", str(worktree_path)], check=False)
            if worktree_path.exists():
                shutil.rmtree(worktree_path, ignore_errors=True)


def main():
    parser = argparse.ArgumentParser(description="Deploy MLB HR Props to GitHub Pages")
    parser.add_argument("--skip-pipeline", action="store_true", help="Skip running pipeline.py")
    parser.add_argument("--dry-run", action="store_true", help="Build only, no git push")
    args = parser.parse_args()

    print("=" * 50)
    print("  MLB HR Props — Deploy")
    print("=" * 50)

    if not args.skip_pipeline:
        print("\n[0/3] Running pipeline…")
        result = subprocess.run([sys.executable, "pipeline.py"])
        if result.returncode != 0:
            print("  Pipeline failed — aborting deploy")
            sys.exit(1)

    build()
    deploy(dry_run=args.dry_run)

    print("\n  Done.")


if __name__ == "__main__":
    main()
