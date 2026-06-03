<script>
  import {
    displayOrderPrice,
    formatMoney,
    formatPrice,
    formatQuantity,
    formatSignedMoney,
    moneyTone
  } from '../formatting.js';
  import {
    fillExitPnlLabel,
    fillStrategyLabel,
    marketDirectionArrow,
    marketDirectionClass,
    orderFillSpreadLabel,
    orderSpreadLabel,
    orderTriggerDetail
  } from '../reviews.js';
  import { formatTimestamp } from '../status.js';
  import {
    hasInstructionAction,
    instructionGuidance,
    instructionOrderDisplay,
    instructionPrimaryAction,
    instructionWindowState,
    positionExitPlan,
    rlCandidateModelId,
    rlCandidateWindowDisplay
  } from '../instructions.js';

  export let positions;
  export let filteredPositions;
  export let openOrders;
  export let filteredOpenOrders;
  export let recentFills;
  export let filteredRecentFills;
  export let rlCandidateInstructions;
  export let executionInstructions;
  export let filteredInstructions;
  export let dashboardFilters;
  export let orderRowActionResult;
  export let instructionRowActionResult;
  export let resetFilterSection;
  export let sectionHasActiveFilters;
  export let buttonStateClass;
  export let buttonIsBusy;
  export let buttonLabel;
  export let enhanceDashboardAction;

  function touchFilters() {
    dashboardFilters = { ...dashboardFilters };
  }
</script>

  <section class="panel" id="positions">
    <div class="panel-head">
      <div>
        <h2>Current Holdings</h2>
        <p>Latest non-zero position snapshots persisted in the ledger.</p>
      </div>
      <div class="panel-tools">
        <span class="subtle">{filteredPositions.length} of {positions.length} visible</span>
        <button
          class="inline-button neutral"
          type="button"
          on:click={() => resetFilterSection('positions')}
          disabled={!sectionHasActiveFilters('positions')}
        >
          Clear Filters
        </button>
      </div>
    </div>
    {#if positions.length === 0}
      <p class="empty">No durable open positions are available yet.</p>
    {:else if filteredPositions.length === 0}
      <p class="empty">
        No holdings match the active filters. Clear filters to show all {positions.length}
        holdings.
      </p>
    {:else}
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Account</th>
              <th>Symbol</th>
              <th>Exchange</th>
              <th>Currency</th>
              <th>Quantity</th>
              <th>Average Cost</th>
              <th>Market Price</th>
              <th>Market Value</th>
              <th>Unrealized PnL</th>
              <th>Exit Plan</th>
            </tr>
            <tr class="filter-row">
              <th><input on:input={touchFilters} bind:value={dashboardFilters.positions.account} placeholder="Filter" /></th>
              <th><input on:input={touchFilters} bind:value={dashboardFilters.positions.symbol} placeholder="Filter" /></th>
              <th><input on:input={touchFilters} bind:value={dashboardFilters.positions.exchange} placeholder="Filter" /></th>
              <th><input on:input={touchFilters} bind:value={dashboardFilters.positions.currency} placeholder="Filter" /></th>
              <th><input on:input={touchFilters} bind:value={dashboardFilters.positions.quantity} placeholder="Filter" /></th>
              <th><input on:input={touchFilters} bind:value={dashboardFilters.positions.averageCost} placeholder="Filter" /></th>
              <th><input on:input={touchFilters} bind:value={dashboardFilters.positions.marketPrice} placeholder="Filter" /></th>
              <th><input on:input={touchFilters} bind:value={dashboardFilters.positions.marketValue} placeholder="Filter" /></th>
              <th><input on:input={touchFilters} bind:value={dashboardFilters.positions.unrealizedPnl} placeholder="Filter" /></th>
              <th><input on:input={touchFilters} bind:value={dashboardFilters.positions.exitPlan} placeholder="Filter" /></th>
            </tr>
          </thead>
          <tbody>
            {#each filteredPositions as position}
              {@const exitPlan = positionExitPlan(position, executionInstructions)}
              <tr>
                <td>
                  {position.account_label ?? position.account_key}
                  {#if position.is_virtual}<span class="mini-badge">Virtual</span>{/if}
                </td>
                <td>{position.local_symbol ?? position.symbol}</td>
                <td>{position.primary_exchange ?? position.exchange}</td>
                <td>{position.currency}</td>
                <td>{formatQuantity(position.quantity)}</td>
                <td>{formatPrice(position.average_cost)}</td>
                <td>{formatPrice(position.market_price)}</td>
                <td>{formatMoney(position.market_value)}</td>
                <td>{formatMoney(position.unrealized_pnl)}</td>
                <td>
                  <span class={`pill ${exitPlan.className}`}>{exitPlan.label}</span>
                  <small class="row-detail">{exitPlan.detail}</small>
                  {#if exitPlan.instructionId}
                    <small class="row-detail mono">{exitPlan.instructionId}</small>
                  {/if}
                </td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    {/if}
  </section>

  <section class="panel" id="orders">
    <div class="panel-head">
      <div>
        <h2>Open Orders</h2>
        <p>Durable broker-order rows that are still operationally open.</p>
      </div>
      <div class="panel-tools">
        <span class="subtle">{filteredOpenOrders.length} of {openOrders.length} visible</span>
        <button
          class="inline-button neutral"
          type="button"
          on:click={() => resetFilterSection('openOrders')}
          disabled={!sectionHasActiveFilters('openOrders')}
        >
          Clear Filters
        </button>
      </div>
    </div>
    {#if orderRowActionResult}
      <p class={`action-feedback ${orderRowActionResult.ok ? 'ok' : 'bad'}`}>
        {orderRowActionResult.message}
      </p>
    {/if}
    {#if openOrders.length === 0}
      <p class="empty">No open broker orders are persisted right now.</p>
    {:else if filteredOpenOrders.length === 0}
      <p class="empty">
        No open orders match the active filters. Clear filters to show all {openOrders.length}
        open orders.
      </p>
    {:else}
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Account</th>
              <th>Symbol</th>
              <th>Role</th>
              <th>Purpose</th>
              <th>Side</th>
              <th>Quantity</th>
              <th>Type</th>
              <th>Limit</th>
              <th>Stop</th>
              <th>Vs Fill</th>
              <th>Market</th>
              <th>Trigger Gap</th>
              <th>Status</th>
              <th>Warning</th>
              <th>Action</th>
            </tr>
            <tr class="filter-row">
              <th><input on:input={touchFilters} bind:value={dashboardFilters.openOrders.account} placeholder="Filter" /></th>
              <th><input on:input={touchFilters} bind:value={dashboardFilters.openOrders.symbol} placeholder="Filter" /></th>
              <th><input on:input={touchFilters} bind:value={dashboardFilters.openOrders.role} placeholder="Filter" /></th>
              <th><input on:input={touchFilters} bind:value={dashboardFilters.openOrders.purpose} placeholder="Filter" /></th>
              <th><input on:input={touchFilters} bind:value={dashboardFilters.openOrders.side} placeholder="Filter" /></th>
              <th><input on:input={touchFilters} bind:value={dashboardFilters.openOrders.quantity} placeholder="Filter" /></th>
              <th><input on:input={touchFilters} bind:value={dashboardFilters.openOrders.type} placeholder="Filter" /></th>
              <th><input on:input={touchFilters} bind:value={dashboardFilters.openOrders.limit} placeholder="Filter" /></th>
              <th><input on:input={touchFilters} bind:value={dashboardFilters.openOrders.stop} placeholder="Filter" /></th>
              <th><input on:input={touchFilters} bind:value={dashboardFilters.openOrders.vsFill} placeholder="Filter" /></th>
              <th><input on:input={touchFilters} bind:value={dashboardFilters.openOrders.market} placeholder="Filter" /></th>
              <th><input on:input={touchFilters} bind:value={dashboardFilters.openOrders.vsMkt} placeholder="Filter" /></th>
              <th><input on:input={touchFilters} bind:value={dashboardFilters.openOrders.status} placeholder="Filter" /></th>
              <th><input on:input={touchFilters} bind:value={dashboardFilters.openOrders.warning} placeholder="Filter" /></th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {#each filteredOpenOrders as order}
              <tr>
                <td>
                  {order.account_label ?? order.account_key}
                  {#if order.is_virtual}<span class="mini-badge">Virtual</span>{/if}
                </td>
                <td>{order.local_symbol ?? order.symbol}</td>
                <td>{order.order_role}</td>
                <td>{order.order_purpose ?? 'n/a'}</td>
                <td>{order.side}</td>
                <td>{formatQuantity(order.total_quantity)}</td>
                <td>{order.order_type}</td>
                <td>{displayOrderPrice(order.limit_price)}</td>
                <td>{displayOrderPrice(order.stop_price)}</td>
                <td>
                  {#if order.fill_basis_price}
                    <div>{orderFillSpreadLabel(order)}</div>
                    <small class="row-detail">
                      from {formatPrice(order.fill_basis_price)}
                      {#if order.fill_basis_at}
                        at {formatTimestamp(order.fill_basis_at)}
                      {/if}
                    </small>
                  {:else}
                    <span class="subtle">n/a</span>
                  {/if}
                </td>
                <td>
                  {#if order.reference_market_price}
                    <div class="market-cell">
                      <span>{formatPrice(order.reference_market_price)}</span>
                      {#if marketDirectionArrow(order.last_market_price_direction)}
                        <span class={`market-arrow ${marketDirectionClass(order.last_market_price_direction)}`}>
                          {marketDirectionArrow(order.last_market_price_direction)}
                        </span>
                      {/if}
                    </div>
                    {#if order.reference_market_price_at}
                      <small class="row-detail">as of {formatTimestamp(order.reference_market_price_at)}</small>
                    {/if}
                  {:else}
                    <span class="subtle">n/a</span>
                  {/if}
                </td>
                <td>
                  <div>{orderSpreadLabel(order)}</div>
                  {#if orderTriggerDetail(order)}
                    <small class="row-detail">{orderTriggerDetail(order)}</small>
                  {/if}
                </td>
                <td>{order.status}</td>
                <td>{order.reject_reason ?? order.warning_text ?? 'n/a'}</td>
                <td>
                  {#if order.external_order_id}
                    <form
                      method="POST"
                      action="?/orderRowAction"
                      class="inline-action-form"
                      use:enhance={enhanceDashboardAction(`cancel-order-${order.external_order_id}`)}
                    >
                      <input type="hidden" name="external_order_id" value={order.external_order_id} />
                      <button
                        class={`inline-button danger ${buttonStateClass(`cancel-order-${order.external_order_id}`)}`}
                        type="submit"
                        data-action-key={`cancel-order-${order.external_order_id}`}
                        disabled={buttonIsBusy(`cancel-order-${order.external_order_id}`)}
                      >
                        {buttonLabel(`cancel-order-${order.external_order_id}`, 'Cancel Order')}
                      </button>
                    </form>
                  {:else}
                    <span class="subtle">No action</span>
                  {/if}
                </td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    {/if}
  </section>

  <section class="panel" id="fills">
    <div class="panel-head">
      <div>
        <h2>Recent Fills</h2>
        <p>Latest persisted execution fills.</p>
      </div>
      <div class="panel-tools">
        <span class="subtle">{filteredRecentFills.length} of {recentFills.length} visible</span>
        <button
          class="inline-button neutral"
          type="button"
          on:click={() => resetFilterSection('recentFills')}
          disabled={!sectionHasActiveFilters('recentFills')}
        >
          Clear Filters
        </button>
      </div>
    </div>
    {#if recentFills.length === 0}
      <p class="empty">No execution fills have been recorded yet.</p>
    {:else if filteredRecentFills.length === 0}
      <p class="empty">
        No fills match the active filters. Clear filters to show all {recentFills.length}
        fills.
      </p>
    {:else}
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Time</th>
              <th>Account</th>
              <th>Symbol</th>
              <th>Side</th>
              <th>Strat</th>
              <th>Quantity</th>
              <th>Price</th>
              <th>Fee</th>
              <th>PnL</th>
            </tr>
            <tr class="filter-row">
              <th><input on:input={touchFilters} bind:value={dashboardFilters.recentFills.time} placeholder="Filter" /></th>
              <th><input on:input={touchFilters} bind:value={dashboardFilters.recentFills.account} placeholder="Filter" /></th>
              <th><input on:input={touchFilters} bind:value={dashboardFilters.recentFills.symbol} placeholder="Filter" /></th>
              <th><input on:input={touchFilters} bind:value={dashboardFilters.recentFills.side} placeholder="Filter" /></th>
              <th><input on:input={touchFilters} bind:value={dashboardFilters.recentFills.strat} placeholder="Filter" /></th>
              <th><input on:input={touchFilters} bind:value={dashboardFilters.recentFills.quantity} placeholder="Filter" /></th>
              <th><input on:input={touchFilters} bind:value={dashboardFilters.recentFills.price} placeholder="Filter" /></th>
              <th><input on:input={touchFilters} bind:value={dashboardFilters.recentFills.fee} placeholder="Filter" /></th>
              <th><input on:input={touchFilters} bind:value={dashboardFilters.recentFills.pnl} placeholder="Filter" /></th>
            </tr>
          </thead>
          <tbody>
            {#each filteredRecentFills as fill}
              <tr>
                <td>{formatTimestamp(fill.executed_at)}</td>
                <td>
                  {fill.account_label ?? fill.account_key}
                  {#if fill.is_virtual}<span class="mini-badge">Virtual</span>{/if}
                </td>
                <td>{fill.symbol}</td>
                <td>{fill.side ?? 'n/a'}</td>
                <td>{fillStrategyLabel(fill)}</td>
                <td>{formatQuantity(fill.quantity)}</td>
                <td>{formatPrice(fill.price)}</td>
                <td>{formatMoney(fill.commission)} {fill.commission_currency ?? ''}</td>
                <td>
                  <span class={moneyTone(fill.realized_pnl)}>
                    {fillExitPnlLabel(fill)}
                  </span>
                  {#if fill.realized_pnl_basis_price}
                    <small class="row-detail">
                      Basis {formatPrice(fill.realized_pnl_basis_price)}
                      {#if fill.realized_pnl_gross && fill.realized_pnl_gross !== fill.realized_pnl}
                        · Gross {formatSignedMoney(fill.realized_pnl_gross)}
                      {/if}
                    </small>
                  {/if}
                </td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    {/if}
  </section>

  <section class="panel" id="rl-candidates">
    <div class="panel-head">
      <div>
        <h2>RL Candidate Feed</h2>
        <p>Daily source names whose model decision window is still scheduled or open.</p>
      </div>
      <div class="panel-tools">
        <span class="subtle">{rlCandidateInstructions.length} active source rows</span>
      </div>
    </div>
    {#if rlCandidateInstructions.length === 0}
      <p class="empty">No active model-routed RL candidates are currently loaded.</p>
    {:else}
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Candidate</th>
              <th>Symbol</th>
              <th>Model</th>
              <th>Account / Book</th>
              <th>Side</th>
              <th>Window</th>
              <th>Queued</th>
            </tr>
          </thead>
          <tbody>
            {#each rlCandidateInstructions as instruction}
              <tr>
                <td class="mono">{instruction.instruction_id}</td>
                <td>{instruction.symbol}</td>
                <td>{rlCandidateModelId(instruction)}</td>
                <td>
                  {instruction.account_key}
                  {#if instruction.is_virtual}<span class="mini-badge">Virtual</span>{/if}
                  <small class="row-detail">{instruction.book_key}</small>
                </td>
                <td>{instruction.side}</td>
                <td>{rlCandidateWindowDisplay(instruction)}</td>
                <td>{formatTimestamp(instruction.updated_at)}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    {/if}
  </section>

  <section class="panel" id="instructions">
    <div class="panel-head">
      <div>
        <h2>Execution Instructions</h2>
        <p>Translated instructions that can be submitted, cancelled, filled, or reconciled.</p>
      </div>
      <div class="panel-tools">
        <span class="subtle">{filteredInstructions.length} of {executionInstructions.length} visible</span>
        <button
          class="inline-button neutral"
          type="button"
          on:click={() => resetFilterSection('instructions')}
          disabled={!sectionHasActiveFilters('instructions')}
        >
          Clear Filters
        </button>
      </div>
    </div>
    {#if instructionRowActionResult}
      <p class={`action-feedback ${instructionRowActionResult.ok ? 'ok' : 'bad'}`}>
        {instructionRowActionResult.message}
      </p>
    {/if}
    {#if executionInstructions.length === 0}
      <p class="empty">No translated execution instructions were found.</p>
    {:else if filteredInstructions.length === 0}
      <p class="empty">
        No execution instructions match the active filters. Clear filters to show all
        {executionInstructions.length} instructions.
      </p>
    {:else}
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Instruction</th>
              <th>Symbol</th>
              <th>State</th>
              <th>Lifecycle</th>
              <th>Guidance</th>
              <th>Entry Order</th>
              <th>Exit Order</th>
              <th>Updated</th>
              <th>Actions</th>
            </tr>
            <tr class="filter-row">
              <th><input on:input={touchFilters} bind:value={dashboardFilters.instructions.instruction} placeholder="Filter" /></th>
              <th><input on:input={touchFilters} bind:value={dashboardFilters.instructions.symbol} placeholder="Filter" /></th>
              <th><input on:input={touchFilters} bind:value={dashboardFilters.instructions.state} placeholder="Filter" /></th>
              <th><input on:input={touchFilters} bind:value={dashboardFilters.instructions.lifecycle} placeholder="Filter" /></th>
              <th><input on:input={touchFilters} bind:value={dashboardFilters.instructions.guidance} placeholder="Filter" /></th>
              <th><input on:input={touchFilters} bind:value={dashboardFilters.instructions.entryOrder} placeholder="Filter" /></th>
              <th><input on:input={touchFilters} bind:value={dashboardFilters.instructions.exitOrder} placeholder="Filter" /></th>
              <th><input on:input={touchFilters} bind:value={dashboardFilters.instructions.updated} placeholder="Filter" /></th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {#each filteredInstructions as instruction}
              {@const windowState = instructionWindowState(instruction)}
              {@const primaryAction = instructionPrimaryAction(instruction)}
              <tr>
                <td class="mono">{instruction.instruction_id}</td>
                <td>{instruction.symbol}</td>
                <td>
                  <span class={`pill ${instruction.state === 'FAILED' || instruction.state === 'NEEDS_REVIEW' ? 'bad' : 'neutral'}`}>
                    {instruction.state}
                  </span>
                </td>
                <td>
                  <span class={`pill ${windowState.className}`}>{windowState.label}</span>
                  <small class="row-detail">{windowState.detail}</small>
                </td>
                <td class="guidance-cell">{instructionGuidance(instruction)}</td>
                <td>{instructionOrderDisplay(instruction, 'entry')}</td>
                <td>{instructionOrderDisplay(instruction, 'exit')}</td>
                <td>{formatTimestamp(instruction.updated_at)}</td>
                <td class="actions-cell">
                  {#if primaryAction && hasInstructionAction(instruction)}
                    <form
                      method="POST"
                      action="?/instructionRowAction"
                      class="inline-action-form"
                      use:enhance={enhanceDashboardAction(`instruction-${instruction.instruction_id}-${primaryAction.operation}`)}
                    >
                      <input type="hidden" name="instruction_id" value={instruction.instruction_id} />
                      <input type="hidden" name="operation" value={primaryAction.operation} />
                      <button
                        class={`${primaryAction.className} ${buttonStateClass(`instruction-${instruction.instruction_id}-${primaryAction.operation}`)}`}
                        type="submit"
                        data-action-key={`instruction-${instruction.instruction_id}-${primaryAction.operation}`}
                        disabled={buttonIsBusy(`instruction-${instruction.instruction_id}-${primaryAction.operation}`)}
                      >
                        {buttonLabel(
                          `instruction-${instruction.instruction_id}-${primaryAction.operation}`,
                          primaryAction.label
                        )}
                      </button>
                    </form>
                  {:else}
                    <span class="subtle">No write action</span>
                  {/if}

                  <a class="inline-button subtle-link" href={`/ledger?instruction_id=${encodeURIComponent(instruction.instruction_id)}`}>
                    Ledger
                  </a>
                </td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    {/if}
  </section>
