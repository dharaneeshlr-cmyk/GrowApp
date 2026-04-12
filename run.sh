#!/bin/bash
# ─────────────────────────────────────────────
#  BudgetCraft — one-click launcher for Mac
#  Double-click this file or run: bash run.sh
# ─────────────────────────────────────────────

set -e
cd "$(dirname "$0")"

echo ""
echo "💰 BudgetCraft — Local Budget Planner"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Create venv if it doesn't exist
if [ ! -d "venv" ]; then
  echo "📦 Creating virtual environment..."
  python3 -m venv venv
fi

# Activate
source venv/bin/activate

# Install / upgrade deps silently
echo "📥 Checking dependencies..."
pip install -q -r requirements.txt

# Open browser after 1.5 second delay (gives Flask time to start)
sleep 1.5 && open http://127.0.0.1:5000 &

echo ""
echo "🚀 Starting server at http://127.0.0.1:5000"
echo "🔑 Login: admin / admindr"
echo "📁 Database: $(pwd)/budget.db"
echo "🛑 Press Ctrl+C to stop"
echo ""

# Start Flask
python app.py
