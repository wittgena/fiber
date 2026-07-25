# topos.tester.agent.behavior
## @lineage: void.topos.tester.agent.behavior
## @lineage: topos.audit.tester.agent.behavior
## @lineage: gov.audit.tester.agent.behavior
## @lineage: audit.tester.agent.behavior
## @lineage: ops.tester.agent.behavior
## @lineage: meta.ops.tester.agent.behavior
## @lineage: agent.loop.tester.behavior
## @lineage: agent.handler.loop.tester.behavior
## @lineage: agent.handler.loop.behavior
import pytest
from arch.contract.schema.graph import EntryNode
from topos.tester.agent.dag import RegulatedSandbox, MetabolicProfile
from arch.topos.node.graph import DagTestReport

@pytest.fixture(scope="module")
def standard_profile():
    """테스트 환경을 위한 대사적 제한(Fixture) 설정"""
    return MetabolicProfile(
        max_threads=1, 
        max_compute_time=2.0, 
        max_simulation_ticks=100
    )

@pytest.fixture
def sandbox(standard_profile):
    """GIVEN: 각 테스트 케이스마다 격리된 샌드박스 환경 제공"""
    return RegulatedSandbox(
        profile=standard_profile, 
        fixed_graph_path="tests/mock_data/mock.bound.json"
    )

@pytest.fixture
def target_entry():
    return EntryNode(entry="auth_module", depth=1)

# ==========================================
# 2. BDD Style Test Cases (Given - When - Then)
# ==========================================

@pytest.mark.asyncio
async def test_agent_schema_should_not_infinite_loop(sandbox, target_entry):
    """
    [BDD Spec]
    GIVEN: 무한 루프 위험이 있는 에이전트 스키마가 주어졌을 때
    WHEN: 샌드박스에서 시뮬레이션을 돌리면
    THEN: 에러가 감지되어 is_valid가 False여야 하며, 대사 비용 한계를 초과해야 한다.
    """
    # GIVEN
    malicious_schema = {
        "nodes": [
            {"id": "node_A", "layer": "tool", "next": "node_B"},
            {"id": "node_B", "layer": "tool", "next": "node_A"} # 무한 루프 발생!
        ]
    }

    # WHEN
    report: DagTestReport = await sandbox.evaluate_safely(malicious_schema, target_entry)

    # THEN (Assertions)
    assert report.is_valid is False, "무한 루프 스키마는 통과되어서는 안 됩니다."
    assert any("Fatigue Exceeded" in err for err in report.simulation_errors)
    assert report.metabolic_cost > 0.0

@pytest.mark.asyncio
async def test_agent_schema_must_align_with_topology(sandbox, target_entry):
    """
    [BDD Spec]
    GIVEN: 코드베이스에 존재하지 않는 환각(Hallucinated) 파일을 참조하는 스키마
    WHEN: 샌드박스에서 검증하면
    THEN: Topology Alignment 에러가 발생해야 한다.
    """
    # GIVEN
    hallucinated_schema = {
        "nodes": [
            {
                "id": "node_read", 
                "layer": "tool", 
                "file_path": "non_existent_file.py" # 모의 지형에 없는 파일
            }
        ]
    }

    # WHEN
    report: DagTestReport = await sandbox.evaluate_safely(hallucinated_schema, target_entry)

    # THEN
    assert report.is_valid is False
    assert any("Hallucination Detected" in err for err in report.alignment_errors)

@pytest.mark.asyncio
async def test_agent_schema_successful_metabolism(sandbox, target_entry):
    """
    [BDD Spec]
    GIVEN: 완벽하게 정합하는 정상적인 스키마
    WHEN: 샌드박스를 통과하면
    THEN: is_valid가 True여야 하며, 대사 비용은 적정 수준(예: 5.0 미만)이어야 한다.
    """
    # GIVEN
    healthy_schema = {
        "nodes": [
            {"id": "node_A", "layer": "llm", "next": "node_B"},
            {"id": "node_B", "layer": "tool", "next": "END"}
        ]
    }

    # WHEN
    report: DagTestReport = await sandbox.evaluate_safely(healthy_schema, target_entry)

    # THEN
    assert report.is_valid is True
    assert len(report.alignment_errors) == 0
    assert len(report.simulation_errors) == 0
    assert report.metabolic_cost < 5.0, "정상 스키마의 연산 비용이 너무 높습니다."