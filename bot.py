import pandas as pd
import numpy as np
import ccxt.async_support as ccxt
import asyncio
import sqlite3
import uvicorn
import threading
from fastapi import FastAPI
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple

# ==========================================
# 0. RENDER HEALTH CHECK SERVER (FASTAPI)
# ==========================================
app = FastAPI()

@app.get("/")
def health_check():
    """Keeps the Render web service container alive."""
    return {
        "status": "ONLINE",
        "engine": "The Quantum-Sentinel V8",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

def start_render_health_gateway():
    """Runs Uvicorn server on a background thread to satisfy Render's port binding."""
    uvicorn.run(app, host="0.0.0.0", port=10000, log_level="warning")

# ==========================================
# 1. DATABASE PERSISTENCE LAYER
# ==========================================
class AdvancedSignalDatabase:
    def __init__(self, db_name="sentinel_quantum_v8.db"):
        self.db_name = db_name
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS quantitative_journal (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT, symbol TEXT, direction TEXT,
                    entry_price REAL, stop_loss REAL, take_profit REAL,
                    final_pnl REAL, trade_status TEXT,
                    feature_choch INTEGER, feature_sweep INTEGER,
                    feature_ob INTEGER, feature_fvg INTEGER, feature_premium INTEGER,
                    calculated_confidence REAL
                )
            """)
            conn.commit()

    def log_trade_intent(self, symbol: str, direction: str, entry: float, sl: float, tp: float, conf: float, f_matrix: Dict):
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO quantitative_journal (
                    timestamp, symbol, direction, entry_price, stop_loss, take_profit,
                    final_pnl, trade_status, feature_choch, feature_sweep, feature_ob,
                    feature_fvg, feature_premium, calculated_confidence
                ) VALUES (?, ?, ?, ?, ?, ?, 0.0, 'OPEN', ?, ?, ?, ?, ?, ?)
            """, (datetime.now(timezone.utc).isoformat(), symbol, direction, entry, sl, tp,
                  f_matrix["choch"], f_matrix["sweep"], f_matrix["ob"], f_matrix["fvg"], f_matrix["bias"], conf))
            conn.commit()

    def query_bayesian_weights(self) -> Dict[str, float]:
        base_weights = {"BIAS": 20.0, "SWEEP": 20.0, "CHOCH": 25.0, "OB": 15.0, "FVG": 20.0}
        with sqlite3.connect(self.db_name) as conn:
            try:
                df = pd.read_sql_query("SELECT * FROM quantitative_journal WHERE trade_status = 'CLOSED'", conn)
            except Exception:
                return base_weights
        if df.empty:
            return base_weights
            
        prior_win_rate = 0.50
        m_weight = 10.0
        calculated_weights = {}
        features = {"feature_choch": "CHOCH", "feature_sweep": "SWEEP", "feature_ob": "OB", "feature_fvg": "FVG", "feature_premium": "BIAS"}
        
        for db_col, weight_name in features.items():
            feature_trades = df[df[db_col] == 1]
            total_trades = len(feature_trades)
            if total_trades > 0:
                wins = len(feature_trades[feature_trades['final_pnl'] > 0])
                bayesian_wr = (wins + (prior_win_rate * m_weight)) / (total_trades + m_weight)
                calculated_weights[weight_name] = float(bayesian_wr * 100)
            else:
                calculated_weights[weight_name] = base_weights[weight_name]
                
        total_sum = sum(calculated_weights.values())
        for k in calculated_weights:
            calculated_weights[k] = (calculated_weights[k] / total_sum) * 100
        return calculated_weights

# ========================================================
# 2. STRUCTURAL AND INSTITUTIONAL ENGINE
# ========================================================
class StructuralStateEngine:
    def __init__(self, sensitivity: int = 3):
        self.sensitivity = sensitivity

    def map_market_geometry(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        if len(df) < 20:
            return df, {"last_hl": None, "last_lh": None, "dealing_high": None, "dealing_low": None, "bos_confirmed": False}
            
        highs, lows, closes = df['high'].values, df['low'].values, df['close'].values
        df['swing_high'] = 0.0
        df['swing_low'] = 0.0
        sh_points, sl_points = [], []
        
        for i in range(self.sensitivity, len(df) - self.sensitivity):
            if all(highs[i] >= highs[i-j] for j in range(1, self.sensitivity+1)) and \
               all(highs[i] > highs[i+j] for j in range(1, self.sensitivity+1)):
                df.at[df.index[i], 'swing_high'] = highs[i]
                sh_points.append(highs[i])
                
            if all(lows[i] <= lows[i-j] for j in range(1, self.sensitivity+1)) and \
               all(lows[i] < lows[i+j] for j in range(1, self.sensitivity+1)):
                df.at[df.index[i], 'swing_low'] = lows[i]
                sl_points.append(lows[i])

        last_valid_hl = sl_points[-2] if len(sl_points) >= 2 else (sl_points[-1] if sl_points else None)
        last_valid_lh = sh_points[-2] if len(sh_points) >= 2 else (sh_points[-1] if sh_points else None)
        dealing_high = sh_points[-1] if sh_points else highs.max()
        dealing_low = sl_points[-1] if sl_points else lows.min()
        
        true_bearish_bos = False
        if len(sl_points) >= 1:
            recent_low_target = sl_points[-1]
            atr_approx = np.mean(highs[-14:] - lows[-14:])
            body_size = abs(closes[-1] - df['open'].iloc[-1])
            if closes[-1] < recent_low_target and body_size > (atr_approx * 0.75):
                true_bearish_bos = True

        state_meta = {
            "last_hl": last_valid_hl, "last_lh": last_valid_lh,
            "dealing_high": dealing_high, "dealing_low": dealing_low,
            "bos_confirmed": true_bearish_bos
        }
        return df, state_meta

class TrueLifecycleScanner:
    @staticmethod
    def calculate_clustered_liquidity(df: pd.DataFrame, threshold=0.0005) -> Dict:
        highs = df[df['swing_high'] > 0]['swing_high'].values
        eqh_clusters = []
        for h in highs:
            if any(abs(h - x) / h < threshold and x != h for x in highs):
                level = float(np.mean([x for x in highs if abs(x - h) / h < threshold]))
                if level not in eqh_clusters: eqh_clusters.append(level)
        return {"EQH": eqh_clusters}

    @staticmethod
    def scan_fvg_depth_profiles(df: pd.DataFrame) -> Dict:
        l, h, c = df['low'].values, df['high'].values, df['close'].values
        open_bear_fvgs = []
        for i in range(2, len(df)):
            if l[i-2] > h[i]:
                gap_top, gap_bottom = l[i-2], h[i]
                ce_midpoint = (gap_top + gap_bottom) / 2
                gap_size = max(gap_top - gap_bottom, 1e-9)
                post_highs = h[i+1:] if i+1 < len(df) else np.array([])
                max_reach = post_highs.max() if post_highs.size > 0 else 0
                if max_reach < gap_top:
                    fill_ratio = ((max_reach - gap_bottom) / gap_size) * 100
                    open_bear_fvgs.append({"top": gap_top, "bottom": gap_bottom, "ce": ce_midpoint, "fill_ratio": fill_ratio, "ce_hit": max_reach >= ce_midpoint})
        current_price = c[-1]
        active_fvg_hit = any(fvg["bottom"] <= current_price <= fvg["top"] for fvg in open_bear_fvgs)
        ce_rejected = any(fvg["ce_hit"] and current_price < fvg["ce"] for fvg in open_bear_fvgs)
        return {"fvg_valid": active_fvg_hit, "ce_rejected": ce_rejected}

    @staticmethod
    def qualify_institutional_blocks(df: pd.DataFrame, state_meta: Dict) -> Dict:
        c, o, h, l, v = df['close'].values, df['open'].values, df['high'].values, df['low'].values, df['volume'].values
        rolling_vol = df['volume'].rolling(20).mean().values
        ob_profile = {"ob_active": False, "breaker_active": False}
        if len(df) < 20 or not state_meta["dealing_high"]:
            return ob_profile

        for i in range(5, len(df) - 1):
            if np.isnan(rolling_vol[i]): continue
            displacement = v[i+1] > rolling_vol[i] * 1.5 and (abs(c[i+1] - o[i+1]) / (h[i+1] - l[i+1])) > 0.60
            if c[i] > o[i] and c[i+1] < l[i] and displacement:
                ob_top, ob_bottom = h[i], l[i]
                if (len(df) - i) > 60: continue
                post_highs = h[i+1:]
                touches = sum(1 for x in post_highs if ob_bottom <= x <= ob_top)
                if ob_bottom <= c[-1] <= ob_top and touches <= 3:
                    ob_profile["ob_active"] = True
                if state_meta["dealing_high"] > ob_top and c[-1] < ob_bottom:
                    ob_profile["breaker_active"] = True
        return ob_profile

# ========================================================
# 3. VOLATILITY RISK & SESSION LAYERS
# ========================================================
class ProductionRiskManager:
    @staticmethod
    def generate_protected_boundaries(df: pd.DataFrame, direction: str, entry: float, state_meta: Dict) -> Tuple[float, float, float]:
        high_low = df['high'] - df['low']
        high_cp = np.abs(df['high'] - df['close'].shift())
        low_cp = np.abs(df['low'] - df['close'].shift())
        tr = pd.concat([high_low, high_cp, low_cp], axis=1).max(axis=1)
        atr = tr.rolling(14).mean().iloc[-1]
        if pd.isna(atr) or atr <= 0:
            atr = float(df['close'].std() * 0.1 if df['close'].std() > 0 else entry * 0.001)
        if direction == "SHORT":
            sl = max(state_meta["dealing_high"], entry + (atr * 1.5))
            tp = entry - (abs(sl - entry) * 3.0)
        else:
            sl = min(state_meta["dealing_low"], entry - (atr * 1.5))
            tp = entry + (abs(entry - sl) * 3.0)
        return float(sl), float(tp), float(atr)

class SessionKillzoneFilter:
    @staticmethod
    def check_killzone() -> Tuple[bool, str]:
        current_hour_utc = datetime.now(timezone.utc).hour
        if 7 <= current_hour_utc <= 10: return True, "LONDON_OPEN"
        elif 12 <= current_hour_utc <= 15: return True, "NEW_YORK_OPEN"
        elif 0 <= current_hour_utc <= 3: return True, "ASIA_ACCUMULATION"
        return False, "OUTSIDE_KILLZONE"

# ========================================================
# 4. CORE ENGINE CORE ORCHESTRATOR
# ========================================================
class CompleteSentinelEngine:
    def __init__(self):
        self.db = AdvancedSignalDatabase()
        self.structure_engine = StructuralStateEngine()
        self.tracked_symbols = ["BTC/USDT", "ETH/USDT"] # Dynamic Token Tracking list

    # METHOD: Coin list se terminate/remove karne ke liye
    def remove_coin(self, symbol: str):
        symbol = symbol.upper()
        if symbol in self.tracked_symbols:
            self.tracked_symbols.remove(symbol)
            print(f"➖ [STOPPED] Logic for {symbol} has been terminated.")
        else:
            print(f"⚠️ [ERROR] {symbol} is not in the active tracking list.")

    # METHOD: Naya coin list me add karne ke liye
    def add_coin(self, symbol: str):
        symbol = symbol.upper()
        if symbol not in self.tracked_symbols:
            self.tracked_symbols.append(symbol)
            print(f"➕ [STARTED] {symbol} is now being monitored by V8 Engine.")
        else:
            print(f"⚠️ [INFO] {symbol} is already being tracked.")

    async def process_market_execution(self, symbol: str, exchange: ccxt.Exchange):
        allowed, session_name = SessionKillzoneFilter.check_killzone()
        if not allowed:
            print(f"⏳ [SESSION_GUARD] Skipping {symbol} (Hour UTC: {datetime.now(timezone.utc).hour}). Outside Volume Killzone.")
            return

        try:
            ltf_ohlcv = await exchange.fetch_ohlcv(symbol, '15m', limit=150)
            htf_ohlcv = await exchange.fetch_ohlcv(symbol, '4h', limit=50)
            if len(ltf_ohlcv) < 30 or len(htf_ohlcv) < 20: return
            
            df_ltf = pd.DataFrame(ltf_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df_htf = pd.DataFrame(htf_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            
            df_ltf, state_meta = self.structure_engine.map_market_geometry(df_ltf)
            closes, volumes = df_ltf['close'].values, df_ltf['volume'].values
            vol_ma = df_ltf['volume'].rolling(20).mean().values
            vol_valid = volumes[-1] > vol_ma[-1] * 1.5 if not np.isnan(vol_ma[-1]) else False
            
            true_bear_choch = False
            if state_meta["last_hl"] and closes[-1] < state_meta["last_hl"] and vol_valid:
                true_bear_choch = True
                
            liquidity = TrueLifecycleScanner.calculate_clustered_liquidity(df_ltf)
            true_bear_sweep = False
            if liquidity["EQH"]:
                max_eqh_target = max(liquidity["EQH"])
                if df_ltf['high'].iloc[-1] > max_eqh_target and closes[-1] < max_eqh_target:
                    true_bear_sweep = True
                    
            fvg_profile = TrueLifecycleScanner.scan_fvg_depth_profiles(df_ltf)
            ob_profile = TrueLifecycleScanner.qualify_institutional_blocks(df_ltf, state_meta)
            equilibrium = (state_meta["dealing_high"] + state_meta["dealing_low"]) / 2
            in_premium_pricing = closes[-1] > equilibrium
            
            calibrated_weights = self.db.query_bayesian_weights()
            features_matrix = {
                "bias": 1 if in_premium_pricing else 0, "sweep": 1 if true_bear_sweep else 0,
                "choch": 1 if (true_bear_choch or state_meta["bos_confirmed"]) else 0,
                "ob": 1 if (ob_profile["ob_active"] or ob_profile["breaker_active"]) else 0,
                "fvg": 1 if (fvg_profile["fvg_valid"] or fvg_profile["ce_rejected"]) else 0
            }
            
            confidence_index = (features_matrix["bias"] * calibrated_weights["BIAS"] +
                                features_matrix["sweep"] * calibrated_weights["SWEEP"] +
                                features_matrix["choch"] * calibrated_weights["CHOCH"] +
                                features_matrix["ob"] * calibrated_weights["OB"] +
                                features_matrix["fvg"] * calibrated_weights["FVG"])

            if confidence_index >= 65.0:
                entry = float(closes[-1])
                sl, tp, atr_val = ProductionRiskManager.generate_protected_boundaries(df_ltf, "SHORT", entry, state_meta)
                self.db.log_trade_intent(symbol, "SHORT", entry, sl, tp, confidence_index, features_matrix)
                
                print(f"\n==========================================================")
                print(f"🚨 [V8 SIGNAL TRIGGERED] SENTINEL EXECUTION ALIGNED")
                print(f"==========================================================")
                print(f"➔ Symbol Node: {symbol} | Active Session Focus: {session_name}")
                print(f"➔ Execution Matrix -> Entry: {entry} | SL: {sl:.2f} | TP: {tp:.2f}")
                print(f"📊 BAYESIAN RESOLUTION CONFLUENCE: {confidence_index:.2f}% CONFIDENCE.")
                print(f"==========================================================\n")
            else:
                print(f"⏳ [SCANNER] {symbol} | Matrix Score: {confidence_index:.2f}% | Searching setup...")
        except Exception as err:
            print(f"❌ [CRITICAL PIPELINE ERROR] Asset {symbol} context faulted: {str(err)}")

    async def engine_core_loop(self):
        """Infinite tracking execution loop running every 5 minutes."""
        # Initial Boot Message Banner
        print("\n" + "="*60)
        print(" 🔥 THE QUANT-SENTINEL V8 ENGINE INITIALIZED SUCCESSFULLY 🔥")
        print("="*60)
        print(f"➔ Boot Timestamp : {datetime.now(timezone.utc).isoformat()}")
        print("➔ Network Mode   : CCXT Async Engine Connected")
        print("➔ Guard Server   : FastAPI Live Gateway Port 10000 Active")
        print("➔ Engine Sandbox : Multi-Timeframe Bayesian Optimization Active")
        print("="*60 + "\n")

        exchange_client = ccxt.okx({"enableRateLimit": True})
        try:
            while True:
                # REFINEMENT: `list(...)` use kiya hai taaki live loops me element remove karne se track crash na kare
                for symbol in list(self.tracked_symbols):
                    await self.process_market_execution(symbol, exchange_client)
                await asyncio.sleep(300) # Tick sequence: 5 minutes
        except Exception as loop_err:
            print(f"💥 Critical Error in main execution engine loop: {str(loop_err)}")
        finally:
            await exchange_client.close()

# ========================================================
# 5. ENTRY POINT PROCESS ROUTER
# ========================================================
if __name__ == "__main__":
    # Step 1: Start Render health validation proxy on background daemon thread
    server_thread = threading.Thread(target=start_render_health_gateway, daemon=True)
    server_thread.start()
    
    # Step 2: Fire Up Main Async Quant Processing Core Pipeline
    asyncio.run(CompleteSentinelEngine().engine_core_loop())
