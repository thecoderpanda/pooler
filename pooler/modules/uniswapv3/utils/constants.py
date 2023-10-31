from web3 import Web3

from pooler.modules.uniswapv3.settings.config import settings as worker_settings
from pooler.utils.default_logger import logger
from pooler.utils.file_utils import read_json_file
from pooler.utils.rpc import RpcHelper

max_gas_static_call = 30_000_000_000

univ3_helper_bytecode = "0x608080604052600436101561001357600080fd5b60003560e01c63802036f51461002857600080fd5b346106155760207ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffc360112610615576004359073ffffffffffffffffffffffffffffffffffffffff82168203610615577fd0c93a7c00000000000000000000000000000000000000000000000000000000815260208160048173ffffffffffffffffffffffffffffffffffffffff86165afa90811561062257600091610975575b506040517f3850c7bd00000000000000000000000000000000000000000000000000000000815260408160048173ffffffffffffffffffffffffffffffffffffffff87165afa80156106225761092f575b508060020b15610900578060020b621b13d10560020b7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe061017361015d83610a1f565b9261016b60405194856109af565b808452610a1f565b013660208301376000918060020b7ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff276180560020b60081d60010b5b8060010b8260020b620d89e80560020b60081d60010b811361062e57604051907f5339c296000000000000000000000000000000000000000000000000000000008252600482015260208160248173ffffffffffffffffffffffffffffffffffffffff8a165afa908115610622576000916105eb575b50805b61026e575060010b617fff811461023f576001016101ad565b7f4e487b7100000000000000000000000000000000000000000000000000000000600052601160045260246000fd5b8060ff6fffffffffffffffffffffffffffffffff8216156105df5750607f5b67ffffffffffffffff8216156105d55760ff7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffc0818316011161023f5760ff167fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffc0015b63ffffffff8216156105cb5760ff7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe0818316011161023f5760ff167fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe0015b61ffff8216156105c15760ff7ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff0818316011161023f5760ff167ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff0015b60ff8216156105b75760ff7ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff8818316011161023f5760ff167ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff8015b600f8216156105ad5760ff7ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffc818316011161023f5760ff167ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffc015b60038216156105a15760ff7ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe818316011161023f577ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe60ff6001921601915b1661054b575b60ff16906001821b18908360020b9060020b8360081b60020b1760020b028060020b90810361023f57807ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff27618839212158061053e575b610524575b50610226565b61053761053088610a37565b9787610a64565b523861051e565b50620d89e8811315610519565b60ff7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff818316011161023f5760ff167fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff016104c4565b9060019060021c6104be565b9060041c9061045f565b9060081c90610404565b9060101c906103a9565b9060201c9061034d565b9060401c906102ef565b90508160801c9061028d565b90506020813d60201161061a575b81610606602093836109af565b81010312610615575138610223565b600080fd5b3d91506105f9565b6040513d6000823e3d90fd5b50505061063a82610a1f565b9261064860405194856109af565b8284527fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe061067584610a1f565b0160005b8181106108ef57505060005b83811061075b578460405160208101916020825280518093526040820192602060408260051b8501019201906000945b8186106106c25784840385f35b9091927fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffc0858203018252835180519081835260005b828110610746575050602080837fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe0601f85600085809860019a01015201160101950192019501949190916106b5565b806020809284010151828287010152016106f7565b6107658184610a64565b5160020b90604051917ff30dba9300000000000000000000000000000000000000000000000000000000835260048301526101008260248173ffffffffffffffffffffffffffffffffffffffff87165afa91821561062257600092610857575b506107d08185610a64565b516040519260801b602084015260e81b603083015260138252604082019180831067ffffffffffffffff84111761082857610823926040526108128288610a64565b5261081d8187610a64565b50610a37565b610685565b7f4e487b7100000000000000000000000000000000000000000000000000000000600052604160045260246000fd5b909150610100813d610100116108e7575b8161087661010093836109af565b810103126106155780516fffffffffffffffffffffffffffffffff8116036106155760208101519081600f0b82036106155760808101518060060b03610615576108c260a082016109fe565b5060c081015163ffffffff8116036106155760e00151801515036106155790386107c5565b3d9150610868565b806060602080938901015201610679565b7f4e487b7100000000000000000000000000000000000000000000000000000000600052601260045260246000fd5b6040813d60401161096d575b81610948604093836109af565b810103126106155760208161095f610966936109fe565b50016109f0565b503861011a565b3d915061093b565b90506020813d6020116109a7575b81610990602093836109af565b81010312610615576109a1906109f0565b386100c9565b3d9150610983565b90601f7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe0910116810190811067ffffffffffffffff82111761082857604052565b51908160020b820361061557565b519073ffffffffffffffffffffffffffffffffffffffff8216820361061557565b67ffffffffffffffff81116108285760051b60200190565b7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff811461023f5760010190565b8051821015610a785760209160051b010190565b7f4e487b7100000000000000000000000000000000000000000000000000000000600052603260045260246000fdfea2646970667358221220b066db1595cf7acb7b895611dc9837f3bd0731b073eef99e736694005439e20864736f6c63430008130033"

constants_logger = logger.bind(module="PowerLoom|Uniswap|Constants")
# Getting current node

rpc_helper = RpcHelper()
current_node = rpc_helper.get_current_node()

# LOAD ABIs
pair_contract_abi = read_json_file(
    worker_settings.uniswap_contract_abis.pair_contract,
    constants_logger,
)
erc20_abi = read_json_file(
    worker_settings.uniswap_contract_abis.erc20,
    constants_logger,
)
router_contract_abi = read_json_file(
    worker_settings.uniswap_contract_abis.router,
    constants_logger,
)
uniswap_trade_events_abi = read_json_file(
    worker_settings.uniswap_contract_abis.trade_events,
    constants_logger,
)
factory_contract_abi = read_json_file(
    worker_settings.uniswap_contract_abis.factory,
    constants_logger,
)

# load helper abi
helper_contract_abi = read_json_file(
    "pooler/tests/static/abi/UniV3Helper.json",
)

override_address = Web3.to_checksum_address("0x" + "1" * 40)

# Init Uniswap V2 Core contract Objects
router_contract_obj = current_node["web3_client"].eth.contract(
    address=Web3.to_checksum_address(
        worker_settings.contract_addresses.uniswap_v3_router,
    ),
    abi=router_contract_abi,
)
factory_contract_obj = current_node["web3_client"].eth.contract(
    address=Web3.to_checksum_address(
        worker_settings.contract_addresses.uniswap_v3_factory,
    ),
    abi=factory_contract_abi,
)
dai_eth_contract_obj = current_node["web3_client"].eth.contract(
    address=Web3.to_checksum_address(
        worker_settings.contract_addresses.DAI_WETH_PAIR,
    ),
    abi=pair_contract_abi,
)
usdc_eth_contract_obj = current_node["web3_client"].eth.contract(
    address=Web3.to_checksum_address(
        worker_settings.contract_addresses.USDC_WETH_PAIR,
    ),
    abi=pair_contract_abi,
)
eth_usdt_contract_obj = current_node["web3_client"].eth.contract(
    address=Web3.to_checksum_address(
        worker_settings.contract_addresses.USDT_WETH_PAIR,
    ),
    abi=pair_contract_abi,
)


# FUNCTION SIGNATURES and OTHER CONSTANTS
UNISWAP_TRADE_EVENT_SIGS = {
    "Swap": "Swap(address,uint256,uint256,uint256,uint256,address)",
    "Mint": "Mint(address,uint256,uint256)",
    "Burn": "Burn(address,uint256,uint256,address)",
}
UNISWAP_EVENTS_ABI = {
    "Swap": usdc_eth_contract_obj.events.Swap._get_event_abi(),
    "Mint": usdc_eth_contract_obj.events.Mint._get_event_abi(),
    "Burn": usdc_eth_contract_obj.events.Burn._get_event_abi(),
}
tokens_decimals = {
    "USDT": 6,
    "DAI": 18,
    "USDC": 6,
    "WETH": 18,
}
