# fiber.llm.provider.cost.adapter
from typing import Dict, Any
from fiber.llm.provider.cost.schema import ModelCostSchema
from fiber.llm.provider.adapter.openrouter import OpenRouterModelData
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("provider.cost.adapter")

class RegistryAdapters:
    
    @staticmethod
    def parse_litellm(raw_json: Dict[str, Any]) -> Dict[str, Dict]:
        log.info(f"[LiteLLM] Starting parsing for {len(raw_json)} raw items.")
        normalized = {}
        
        success = 0
        skipped_modality = 0      # Non-text models (image, video, etc.)
        skipped_missing_cost = 0  # Missing text token cost
        failed = 0                # Actual schema parsing errors
        
        for model_id, obj in raw_json.items():
            if model_id == "sample_spec": 
                continue
                
            # 1. Skip non-text generation modes
            if obj.get("mode") not in ("chat", None, "completion"):
                skipped_modality += 1
                continue
                
            # 2. System strictly requires text token costs.
            # Gracefully skip audio/video specific models that lack text token cost fields.
            if "input_cost_per_token" not in obj and "output_cost_per_token" not in obj:
                if "input_cost_per_video_per_second" in obj or "input_cost_per_audio_per_second" in obj:
                    skipped_modality += 1
                else:
                    skipped_missing_cost += 1
                continue
                
            # 3. Validate and map schema
            try:
                schema_inst = ModelCostSchema.from_dict(obj)
                normalized[model_id] = schema_inst.to_clean_dict()
                success += 1
            except Exception as e:
                failed += 1
                # Sample actual errors to prevent log flooding
                if failed <= 3:
                    log.debug(f"[LiteLLM] Mapping failed for '{model_id}' | Error: {e} | Raw Data: {obj}")
                
        if failed > 3:
            log.debug(f"[LiteLLM] ... and {failed - 3} more mapping errors suppressed.")
            
        log.info(
            f"[LiteLLM] Parsing complete -> "
            f"Success: {success}, "
            f"Skipped(Modality): {skipped_modality}, "
            f"Skipped(NoCost): {skipped_missing_cost}, "
            f"Failed: {failed}"
        )
        return normalized

    @staticmethod
    def parse_openrouter(raw_json: Dict[str, Any]) -> Dict[str, Dict]:
        data_list = raw_json.get("data", [])
        log.info(f"[OpenRouter] Starting parsing for {len(data_list)} raw items.")
        
        normalized = {}
        success, skipped, failed = 0, 0, 0
        
        for obj in data_list:
            try:
                # Safely convert using the dedicated OpenRouter dataclass
                or_model = OpenRouterModelData.from_dict(obj)
                universal_schema = or_model.to_universal_schema()
                
                normalized[or_model.id] = universal_schema.to_clean_dict()
                success += 1
            except KeyError as e:
                skipped += 1
                if skipped <= 3:
                    log.debug(f"[OpenRouter] Skipped item | Reason: {e}")
            except Exception as e:
                failed += 1
                if failed <= 3:
                    log.debug(f"[OpenRouter] Mapping failed for '{obj.get('id', 'Unknown')}' | Error: {e}")
                
        if failed > 3:
            log.debug(f"[OpenRouter] ... and {failed - 3} more mapping errors suppressed.")
            
        log.info(f"[OpenRouter] Parsing complete -> Success: {success}, Skipped: {skipped}, Failed: {failed}")
        return normalized