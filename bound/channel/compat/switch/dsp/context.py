# bound.channel.compat.switch.dsp.context
import copy
from bound.channel.compat.switch.dsp.settings import settings

def get_dspy_context_propagator():
    """
    @role: 메인 스레드의 DSPy 설정을 캡처하여 하위 스레드로 주입하는 지연 실행기
    @flow: Evaluate나 ParallelRunner가 OptExecutor를 호출할 때 주입용으로 사용
    """
    current_config = settings.copy()
    
    # usage_tracker 등 스레드간 완벽한 격리가 필요한 객체는 깊은 복사 처리
    if current_config.get("usage_tracker"):
        current_config["usage_tracker"] = copy.deepcopy(current_config["usage_tracker"])
        
    return lambda: settings.context(**current_config)