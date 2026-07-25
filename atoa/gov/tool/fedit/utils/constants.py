# atoa.gov.tool.fedit.utils.constants
## @lineage: agent.gov.tool.fedit.utils.constants
## @lineage: gov.sandbox.engine.tool.fedit.utils.constants
## @lineage: agent.engine.tool.fedit.utils.constants
## @lineage: sandbox.tool.fedit.utils.constants
## @lineage: ops.xor.tool.fedit.utils.constants
## @lineage: meta.xor.tool.fedit.utils.constants
## @lineage: gov.engine.tool.fedit.utils.constants
## @lineage: gov.engine.executor.tool.fedit.utils.constants
## @lineage: gov.executor.tool.fedit.utils.constants
## @lineage: gov.executor.fedit.utils.constants
## @lineage: gov.sandbox.executor.fedit.utils.constants
## @lineage: gov.sphere.sandbox.executor.fedit.utils.constants
## @lineage: gov.medium.tool.fedit.utils.constants
## @lineage: gov.bridge.tool.fedit.utils.constants
## @lineage: meta.ops.tool.fedit.utils.constants
## @lineage: meta.loop.tool.fedit.utils.constants
## @lineage: workspace.tool.fedit.utils.constants
## @lineage: xyz.workspace.tool.fedit.utils.constants
## @lineage: bridge.tool.fedit.utils.constants
## @lineage: foldbox.tool.fedit.utils.constants
## @lineage: ator.tool.fedit.utils.constants
## @lineage: ator.tools.fedit.utils.constants
## @lineage: foldbox.flow.tools.fedit.utils.constants
## @lineage: foldbox.tools.fedit.utils.constants
## @lineage: agent.tools.fedit.utils.constants
## @lineage: loop.tools.fedit.utils.constants
## @lineage: tools.fedit.utils.constants
## @lineage: tools.file_editor.utils.constants
## @lineage: bridge.quarantine.tools.file_editor.utils.constants
MAX_RESPONSE_LEN_CHAR: int = 16000

CONTENT_TRUNCATED_NOTICE = "<response clipped><NOTE>Due to the max output limit, only part of the full response has been shown to you.</NOTE>"  # noqa: E501

TEXT_FILE_CONTENT_TRUNCATED_NOTICE: str = "<response clipped><NOTE>Due to the max output limit, only part of this file has been shown to you. You should retry this tool after you have searched inside the file with `grep -n` in order to find the line numbers of what you are looking for.</NOTE>"  # noqa: E501

BINARY_FILE_CONTENT_TRUNCATED_NOTICE: str = "<response clipped><NOTE>Due to the max output limit, only part of this file has been shown to you. Please use Python libraries to view the entire file or search for specific content within the file.</NOTE>"  # noqa: E501

DIRECTORY_CONTENT_TRUNCATED_NOTICE: str = "<response clipped><NOTE>Due to the max output limit, only part of this directory has been shown to you. You should use `ls -la` instead to view large directories incrementally.</NOTE>"  # noqa: E501
