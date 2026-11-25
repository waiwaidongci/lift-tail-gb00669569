import {api} from '../api/client.js';
import {errorSlot} from '../components/common/layout.js';
import {table} from '../components/common/table.js';
import {state} from '../stores/session.js';
import {esc} from '../utils/html.js';

const PERIOD_KEYS = [
  ['morning', '进场装货'],
  ['midday', '展中补货'],
  ['evening', '撤展卸货'],
];

function parseCorrectionText(text) {
  const trimmed = text.trim();
  if (!trimmed) throw new Error('请填写或选择更正记录。');
  if (trimmed.startsWith('[') || trimmed.startsWith('{')) {
    const parsed = JSON.parse(trimmed);
    return Array.isArray(parsed) ? parsed : [parsed];
  }
  const lines = trimmed.split(/\r?\n/).filter(Boolean);
  if (lines.length < 2) throw new Error('CSV 至少需要表头和一条更正记录。');
  const headers = lines[0].split(',').map(cell => cell.trim());
  return lines.slice(1).map(line => {
    const cells = line.split(',');
    return Object.fromEntries(headers.map((key, index) => {
      let value = (cells[index] || '').trim();
      if (value !== '' && !Number.isNaN(Number(value))) value = Number(value);
      return [key, value];
    }));
  });
}

export async function reservationsPanel() {
  let box;
  try {
    box = await api('/reservations/rolling');
  } catch (error) {
    return `<section><h2>按日滚动七天预留工时</h2><p class="error" role="alert">${esc(error.message)}</p></section>`;
  }

  const current = box.current;
  const computing = ['queued', 'computing'].includes(box.state);
  const statusLine = computing
    ? `<p class="notice" data-poll role="status">计算中：更正后的窗口正在算齐；以下仍是完整旧版（v${esc(box.version)}）。</p>`
    : box.state === 'failed'
      ? `<p class="error" role="alert">最近一次计算失败：${esc(box.last_error || '未知错误')}。以下仍可查看完整旧版（v${esc(box.version)}）。</p>`
      : `<p class="hint" role="status">当前完整版本 v${esc(box.version)} · 修订栅栏 ${esc(box.fence)} · 七日滚动窗口</p>`;

  const windows = box.daily?.length ? box.daily : (current?.windows || []);
  const rows = windows.map(window => ({
    日期: window.window_end,
    窗口开始: window.window_start,
    窗口结束: window.window_end,
    ...Object.fromEntries(PERIOD_KEYS.map(([key, label]) => [
      label,
      window.reservations?.[key] == null ? '—' : `${window.reservations[key]} 分钟`,
    ])),
    状态: window.status === 'ready' ? '已算齐' : window.status === 'insufficient_data' ? '样本不足' : '无记录',
  }));

  const canEdit = state.account.role === 'analyst';
  const form = canEdit ? `<form id="correction-batch" data-correction-form class="correction-form">
      <label>批次号（可留空自动生成）<input name="batch_id" maxlength="80" placeholder="例如 CORR-20261125-01"></label>
      <label>更正记录（JSON 数组，或含 task_id,revision,started_at,duration_minutes,wait_minutes 的 CSV）
        <textarea name="corrections" rows="7" placeholder='[{"task_id":"B-1119-3-01","revision":2,"duration_minutes":82}]'></textarea>
      </label>
      <label>也可选择 JSON/CSV 更正单<input name="correction_file" type="file" accept=".json,.csv,text/csv,application/json"></label>
      <div class="button-row"><button type="submit">整批提交更正单</button><button id="run-reservations" type="button" class="secondary">中断后续算</button></div>
      ${errorSlot}
    </form>` : '<p class="hint">分析员可提交更正单；当前账号可查看预留工时。</p>';

  const batches = (box.batches || []).slice(-5).reverse().map(item => ({
    批次: item.batch_id,
    状态: item.status === 'accepted' ? '已接收' : item.status === 'rejected' ? '整批拒收' : item.status,
    更正数: item.correction_count ?? '—',
    说明: item.error || (item.job_id ? `计算任务 ${item.job_id.slice(0, 8)}` : '未触发计算'),
  }));

  return `<section class="reservation-panel" aria-labelledby="reservation-title">
    <h2 id="reservation-title">按日滚动七天 · 各时段预留工时</h2>
    <p class="hint">每趟以最高修订号为准；同趟同号但内容冲突时整批拒收。跨日更正只使原日期、新日期共同牵连的七日窗口失效，所有窗口算齐后才一次性切换版本。</p>
    ${statusLine}
    <div data-reservation-table>${table(rows)}</div>
    ${form}
    <p id="reservation-action-error" class="error" role="alert"></p>
    ${batches.length ? `<h3>最近更正批次</h3>${table(batches)}` : ''}
  </section>`;
}

export function bindReservations(root, refresh) {
  root.querySelector('#correction-batch')?.addEventListener('submit', async event => {
    event.preventDefault();
    const form = event.currentTarget;
    const error = form.querySelector('[data-error]');
    const button = form.querySelector('button[type="submit"]');
    button.disabled = true;
    if (error) error.textContent = '';
    try {
      const file = form.elements.correction_file?.files?.[0];
      const text = file ? await file.text() : form.elements.corrections.value;
      const corrections = parseCorrectionText(text || '');
      const batch_id = form.elements.batch_id.value.trim() || undefined;
      await api('/correction-batches', {batch_id, corrections});
      state.status = '更正单已接收，窗口结果算齐后自动切换。';
      await refresh();
    } catch (caught) {
      if (error) error.textContent = caught.message;
    } finally {
      button.disabled = false;
    }
  });

  root.querySelector('#run-reservations')?.addEventListener('click', async event => {
    const target = event.currentTarget;
    const error = root.querySelector('#reservation-action-error');
    target.disabled = true;
    if (error) error.textContent = '';
    try {
      await api('/reservations/recompute', {});
      state.status = '中断任务已续算；若已有较新版本，旧结果不会回拨工时。';
      await refresh();
    } catch (caught) {
      if (error) error.textContent = caught.message;
    } finally {
      target.disabled = false;
    }
  });
}
