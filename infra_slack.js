/**
 * The /infra asset-tracker features, on any Bolt app.
 *
 * Moved out of index.js (2026-10-08) so the Domain Suggester app — the one
 * the team can actually edit and DM — offers the same things as Infra Bot:
 *   add / renew / list assets, CSV import / renew / delete, "renew" and
 *   "delete" name lists, and an "expiring soon" view with Mark-renewed.
 * Infra Bot keeps calling the same functions, so behaviour is identical.
 *
 * deps: { Asset, dayjs, axios, syncAllAssetsToSheet, prepareAssetsForSheet,
 *         parseDate, parseCost, resolveOwner, computeDaysLeft, parseCSVBuffer,
 *         buildNameQuery }
 */

const NL = String.fromCharCode(10);

function addAssetModal() {
  const sel = (block_id, label, options, optional = true, initial) => {
    const element = { type: 'static_select', action_id: 'value',
      options: options.map(([t, v]) => ({ text: { type: 'plain_text', text: t }, value: v })) };
    if (initial) element.initial_option = { text: { type: 'plain_text', text: initial[0] }, value: initial[1] };
    return { type: 'input', optional, block_id, label: { type: 'plain_text', text: label }, element };
  };
  const txt = (block_id, label, placeholder, optional = true, multiline = false) => {
    const element = { type: 'plain_text_input', action_id: 'value' };
    if (placeholder) element.placeholder = { type: 'plain_text', text: placeholder };
    if (multiline) element.multiline = true;
    return { type: 'input', optional, block_id, label: { type: 'plain_text', text: label }, element };
  };
  return {
    type: 'modal', callback_id: 'ADD_ASSET_MODAL',
    title: { type: 'plain_text', text: 'Add Asset' },
    submit: { type: 'plain_text', text: 'Save' },
    close: { type: 'plain_text', text: 'Cancel' },
    blocks: [
      sel('type', 'Asset Type', [['Domain', 'DOMAIN'], ['Inbox', 'INBOX']], false),
      txt('name', 'Name (Domain / Email)', 'e.g., example.com or user@example.com', false),
      txt('client', 'Client'),
      txt('provider', 'Provider', 'e.g., Zapmail, GoDaddy'),
      txt('workspace', 'Workspace', 'e.g., Google, Outlook'),
      txt('campaign', 'Campaign'),
      txt('purchaseDate', 'Purchase Date (DD/MM/YYYY)', 'e.g., 04/06/2025'),
      txt('expiryDate', 'Expiry Date (DD/MM/YYYY)', 'Leave blank for auto-calculation'),
      sel('status', 'Status', [['Active', 'Active'], ['Inactive', 'Inactive'], ['Not In Use', 'Not In Use']]),
      sel('brandedPrewarmed', 'Branded / Pre-warmed?', [['Yes — Branded', 'Branded'], ['Yes — Pre-warmed', 'Pre-warmed'], ['No', 'No']]),
      { type: 'input', optional: true, block_id: 'primaryOwner', label: { type: 'plain_text', text: 'Primary Owner' },
        element: { type: 'users_select', action_id: 'value' } },
      { type: 'input', optional: true, block_id: 'visibilityChannel', label: { type: 'plain_text', text: 'Visibility Channel' },
        element: { type: 'channels_select', action_id: 'value' } },
      txt('cost', 'Cost (Yearly for Domains, Monthly for Inboxes)', 'e.g., 3.25'),
      sel('currency', 'Currency', [['USD', 'USD'], ['INR', 'INR'], ['EUR', 'EUR'], ['GBP', 'GBP']], true, ['USD', 'USD']),
      txt('notes', 'Notes', null, true, true)
    ]
  };
}

function renewAssetModal() {
  return {
    type: 'modal', callback_id: 'RENEW_ASSET_MODAL',
    title: { type: 'plain_text', text: 'Renew Asset' },
    submit: { type: 'plain_text', text: 'Renew' },
    close: { type: 'plain_text', text: 'Cancel' },
    blocks: [
      { type: 'input', block_id: 'name', label: { type: 'plain_text', text: 'Domain or Inbox name' },
        element: { type: 'plain_text_input', action_id: 'value', placeholder: { type: 'plain_text', text: 'e.g., example.com or user@example.com' } } },
      { type: 'input', optional: true, block_id: 'purchaseDate', label: { type: 'plain_text', text: 'New Purchase Date (DD/MM/YYYY)' },
        element: { type: 'plain_text_input', action_id: 'value', placeholder: { type: 'plain_text', text: 'e.g., 24/02/2026' } } },
      { type: 'input', optional: true, block_id: 'expiryDate', label: { type: 'plain_text', text: 'New Expiry Date (DD/MM/YYYY)' },
        element: { type: 'plain_text_input', action_id: 'value', placeholder: { type: 'plain_text', text: 'Leave blank to auto-calculate from purchase date' } } }
    ]
  };
}

function makeInfra(deps) {
  const { Asset, dayjs, syncAllAssetsToSheet, prepareAssetsForSheet, parseDate, parseCost,
    resolveOwner, computeDaysLeft, parseCSVBuffer, buildNameQuery } = deps;

  async function resync() {
    const allAssets = await Asset.find();
    await syncAllAssetsToSheet(prepareAssetsForSheet(allAssets));
  }

  async function listText() {
    const prepared = prepareAssetsForSheet(await Asset.find().sort({ type: 1, name: 1 }));
    let message = '📋 *Infrastructure Assets*\n\n';
    const domains = prepared.filter(a => a.type === 'DOMAIN');
    const inboxes = prepared.filter(a => a.type === 'INBOX');
    if (domains.length > 0) {
      message += `*DOMAINS (${domains.length})*\n`;
      domains.forEach(d => { message += `• ${d.name} — ${d.status || 'N/A'} — ${d.daysLeft !== null ? `${d.daysLeft} days left` : 'No expiry'}\n`; });
      message += '\n';
    }
    if (inboxes.length > 0) {
      message += `*INBOXES (${inboxes.length})*\n`;
      inboxes.forEach(i => { message += `• ${i.name} — ${i.status || 'N/A'} — ${i.daysLeft !== null ? `${i.daysLeft} days left` : 'No expiry'}\n`; });
    }
    return message;
  }

  /** Mark names renewed in the tracker: purchase = today, inbox expiry +1 month, domain auto (+365). */
  async function markRenewed(names) {
    let renewed = 0, notFound = 0;
    const today = dayjs().startOf('day');
    for (const name of names) {
      const existing = await Asset.findOne(buildNameQuery(name));
      if (!existing) { notFound++; continue; }
      const newExpiryDate = existing.type === 'INBOX' ? today.add(1, 'month').toDate() : null;
      await Asset.findOneAndUpdate({ name: existing.name }, {
        purchaseDate: today.toDate(), expiryDate: newExpiryDate, remindersSent: [], updatedAt: new Date()
      });
      renewed++;
    }
    await resync();
    return { renewed, notFound };
  }

  async function deleteNames(names) {
    let deleted = 0, notFound = 0;
    for (const name of names) {
      let result = await Asset.deleteOne({ name });
      if (result.deletedCount === 0) result = await Asset.deleteOne(buildNameQuery(name));
      if (result.deletedCount > 0) deleted++; else notFound++;
    }
    await resync();
    return { deleted, notFound };
  }

  /** Active tracker rows expiring within `days` (0 = today), grouped by client. */
  async function expiring(days) {
    const prepared = prepareAssetsForSheet(await Asset.find({ status: 'Active' }));
    return prepared.filter(a => a.daysLeft !== null && a.daysLeft >= 0 && a.daysLeft <= days)
      .sort((a, b) => a.daysLeft - b.daysLeft || String(a.client).localeCompare(String(b.client)) || a.name.localeCompare(b.name));
  }

  /**
   * CSV uploads and "renew"/"delete" name lists in a message.
   * `say(text)` posts the reply where the message came from; `botToken`
   * downloads files (each Slack app has its own).
   * Returns true when the message was an infra command.
   */
  async function handleMessage(message, say, botToken) {
    if (message.files && message.files.length > 0) {
      let handled = false;
      for (const file of message.files) {
        if (!file.mimetype?.includes('csv') && !file.name?.endsWith('.csv')) continue;
        handled = true;
        const fileName = file.name.toLowerCase();
        const isDelete = fileName.startsWith('delete_');
        const isRenew = fileName.startsWith('renew_');
        let assetType = null;
        if (fileName.includes('domain')) assetType = 'DOMAIN';
        else if (fileName.includes('inbox')) assetType = 'INBOX';
        if (!assetType) {
          await say(`Couldn't detect asset type from filename *${file.name}*. Please name your file with "domain" or "inbox" in it.`);
          continue;
        }
        await say(isDelete ? `Detected *${assetType}* delete CSV — removing listed entries from *${file.name}*...`
          : isRenew ? `Detected *${assetType}* renewal CSV — updating dates from *${file.name}*...`
            : `Detected *${assetType}* CSV — importing *${file.name}*...`);
        const response = await deps.axios.get(file.url_private_download || file.url_private, {
          headers: { Authorization: `Bearer ${botToken}` }, responseType: 'arraybuffer' });
        const assets = await parseCSVBuffer(Buffer.from(response.data), assetType);
        if (assets.length === 0) {
          await say(`No valid rows found in *${file.name}*. Check that your column headers match the expected format.`);
          continue;
        }
        if (isRenew) {
          let renewed = 0, notFound = 0;
          for (const asset of assets) {
            const existing = await Asset.findOne({ name: asset.name });
            if (!existing) { notFound++; continue; }
            const updates = { updatedAt: new Date(), remindersSent: [] };
            if (asset.purchaseDate) updates.purchaseDate = asset.purchaseDate;
            if (asset.expiryDate) updates.expiryDate = asset.expiryDate;
            if (asset.purchaseDate && !asset.expiryDate) updates.expiryDate = null;
            await Asset.findOneAndUpdate({ name: asset.name }, updates);
            renewed++;
          }
          await resync();
          await say(`Renewed ${renewed} asset${renewed !== 1 ? 's' : ''}${notFound > 0 ? `, ${notFound} not found` : ''}. Reminder history cleared. Google Sheets synced ✓`);
        } else if (isDelete) {
          let deleted = 0, notFound = 0;
          for (const asset of assets) {
            const result = await Asset.deleteOne({ name: asset.name });
            if (result.deletedCount > 0) deleted++; else notFound++;
          }
          await resync();
          await say(`Deleted ${deleted} asset${deleted !== 1 ? 's' : ''}${notFound > 0 ? `, ${notFound} not found` : ''}. Google Sheets synced ✓`);
        } else {
          let inserted = 0, updated = 0;
          for (const asset of assets) {
            const existing = await Asset.findOne({ name: asset.name });
            if (existing) { await Asset.findOneAndUpdate({ name: asset.name }, asset, { new: true }); updated++; }
            else { await Asset.create(asset); inserted++; }
          }
          await resync();
          await say(`*${file.name}* imported successfully!\n• Inserted: ${inserted}\n• Updated: ${updated}\n• Total: ${assets.length}\n• Google Sheets synced ✓`);
        }
      }
      return handled;
    }
    if (!message.text) return false;
    const lines = message.text.trim().split(/\r?\n/).map(l => l.trim()).filter(Boolean);
    if (lines.length < 2) return false;
    const command = lines[0].toLowerCase();
    if (command !== 'delete' && command !== 'renew') return false;
    const names = lines.slice(1).map(l => {
      const mailtoMatch = l.match(/<mailto:[^|]+\|([^>]+)>/);
      return (mailtoMatch ? mailtoMatch[1] : l).trim().toLowerCase();
    }).filter(Boolean);
    if (command === 'delete') {
      const { deleted, notFound } = await deleteNames(names);
      await say(`Deleted ${deleted} asset${deleted !== 1 ? 's' : ''}${notFound > 0 ? `, ${notFound} not found` : ''}. Google Sheets synced ✓`);
    } else {
      const { renewed, notFound } = await markRenewed(names);
      await say(`Renewed ${renewed} asset${renewed !== 1 ? 's' : ''}${notFound > 0 ? `, ${notFound} not found` : ''}. Purchase date set to today, expiry auto-calculated. Google Sheets synced ✓`);
    }
    return true;
  }

  async function onAddSubmit({ ack, body, view, client }) {
    await ack();
    const v = view.state.values;
    const type = v.type.value.selected_option.value;
    const asset = {
      type,
      name: v.name.value.value.trim().toLowerCase(),
      client: v.client?.value?.value?.trim() || null,
      provider: v.provider?.value?.value?.trim() || null,
      workspace: v.workspace?.value?.value?.trim() || null,
      campaign: v.campaign?.value?.value?.trim() || null,
      purchaseDate: parseDate(v.purchaseDate?.value?.value),
      expiryDate: parseDate(v.expiryDate?.value?.value),
      status: v.status?.value?.selected_option?.value || 'Active',
      brandedPrewarmed: v.brandedPrewarmed?.value?.selected_option?.value || null,
      primaryOwner: resolveOwner(v.primaryOwner?.value?.selected_user) || null,
      visibilityChannel: v.visibilityChannel?.value?.selected_channel || null,
      yearlyCost: type === 'DOMAIN' ? parseCost(v.cost?.value?.value) : null,
      monthlyCost: type === 'INBOX' ? parseCost(v.cost?.value?.value) : null,
      currency: v.currency?.value?.selected_option?.value || 'USD',
      notes: v.notes?.value?.value?.trim() || null,
      createdBy: body.user.id,
      updatedAt: new Date()
    };
    if (type === 'INBOX' && asset.name.includes('@')) asset.domain = asset.name.split('@')[1];
    try {
      await Asset.findOneAndUpdate({ name: asset.name }, asset, { upsert: true, new: true });
      await resync();
      await client.chat.postMessage({ channel: body.user.id,
        text: `✅ *${asset.name}* (${type}) saved successfully!\nStatus: ${asset.status}\n`
          + `Expiry: ${asset.expiryDate ? dayjs(asset.expiryDate).format('DD/MM/YYYY') : 'Auto-calculated'}` });
    } catch (error) {
      console.error('Error saving asset:', error);
      await client.chat.postMessage({ channel: body.user.id, text: `❌ Error saving asset: ${error.message}` });
    }
  }

  async function onRenewSubmit({ ack, body, view, client }) {
    await ack();
    const v = view.state.values;
    const name = v.name.value.value.trim().toLowerCase();
    const newPurchaseDate = parseDate(v.purchaseDate?.value?.value);
    const newExpiryDate = parseDate(v.expiryDate?.value?.value);
    try {
      const asset = await Asset.findOne({ name });
      if (!asset) {
        await client.chat.postMessage({ channel: body.user.id, text: `❌ Asset *${name}* not found in the database. Check the name and try again.` });
        return;
      }
      const updates = { updatedAt: new Date(), remindersSent: [] };
      if (newPurchaseDate) updates.purchaseDate = newPurchaseDate;
      if (newExpiryDate) updates.expiryDate = newExpiryDate;
      if (newPurchaseDate && !newExpiryDate) updates.expiryDate = null;
      await Asset.findOneAndUpdate({ name }, updates);
      await resync();
      const daysLeft = computeDaysLeft(await Asset.findOne({ name }));
      await client.chat.postMessage({ channel: body.user.id,
        text: `✅ *${name}* renewed successfully!\n`
          + `• New Purchase Date: ${newPurchaseDate ? dayjs(newPurchaseDate).format('DD/MM/YYYY') : 'Unchanged'}\n`
          + `• New Expiry Date: ${updates.expiryDate ? dayjs(updates.expiryDate).format('DD/MM/YYYY') : 'Auto-calculated'}\n`
          + `• Days until expiry: ${daysLeft !== null ? daysLeft : 'N/A'}\n• Reminder history cleared ✓\n• Google Sheets synced ✓` });
    } catch (error) {
      console.error('Error renewing asset:', error);
      await client.chat.postMessage({ channel: body.user.id, text: `❌ Error renewing asset: ${error.message}` });
    }
  }

  return { listText, markRenewed, deleteNames, expiring, handleMessage, onAddSubmit, onRenewSubmit };
}

/** "Expiring in N days" with a Mark-renewed button per client group. */
function expiringBlocks(rows, days) {
  const head = '*Tracker: expiring in the next ' + days + ' day' + (days === 1 ? '' : 's') + '* — ' + rows.length + ' active row(s)';
  if (!rows.length) return [{ type: 'section', text: { type: 'mrkdwn', text: head + NL + '_nothing_' } }];
  const byClient = {};
  rows.forEach(r => { (byClient[r.client || 'No client'] = byClient[r.client || 'No client'] || []).push(r); });
  const blocks = [{ type: 'section', text: { type: 'mrkdwn', text: head + NL
    + '_Zapmail and ScaledMail rows renew on their own (subscriptions); *Mark renewed* only updates the tracker after you have renewed or confirmed it._' } }];
  for (const [client, list] of Object.entries(byClient).slice(0, 20)) {
    const lines = list.slice(0, 25).map(a => '• `' + a.name + '` — ' + (a.daysLeft === 0 ? '*today*' : a.daysLeft + 'd')
      + ' · ' + (a.provider || '?') + (a.type === 'INBOX' ? '' : ' · domain'));
    const s = { type: 'section', text: { type: 'mrkdwn', text: ('*' + client + '* (' + list.length + ')' + NL + lines.join(NL)
      + (list.length > 25 ? NL + '…and ' + (list.length - 25) + ' more' : '')).slice(0, 2900) } };
    const names = list.map(a => a.name);
    const value = JSON.stringify({ names });
    if (value.length <= 1900) {
      s.accessory = { type: 'button', action_id: 'infra_mark_renewed', text: { type: 'plain_text', text: 'Mark renewed' }, value,
        confirm: { title: { type: 'plain_text', text: 'Mark renewed in the tracker?' },
          text: { type: 'plain_text', text: 'Sets purchase date to today for ' + names.length + ' row(s) (' + client + '): inboxes expire in 1 month, domains in 1 year. Nothing is paid or changed at the provider.' },
          confirm: { type: 'plain_text', text: 'Mark renewed' }, deny: { type: 'plain_text', text: 'Cancel' } } };
    }
    blocks.push(s);
  }
  return blocks.slice(0, 48);
}

/**
 * Register the tracker features on a Bolt app.
 *   opts.botToken  token for CSV downloads
 *   opts.command   slash command to answer ('/infra'), or null to skip
 *   opts.messages  true = handle CSV / renew / delete messages here
 *                  (Domain Suggester routes DMs itself and calls handleMessage)
 */
/** Would this message change the tracker (CSV upload, or "renew"/"delete" + names)? */
function isInfraMessage(message) {
  if ((message.files || []).some(f => f.mimetype?.includes('csv') || f.name?.endsWith('.csv'))) return true;
  const lines = String(message.text || '').trim().split(/\r?\n/).filter(l => l.trim());
  return lines.length >= 2 && ['delete', 'renew'].includes(lines[0].trim().toLowerCase());
}

const LOCKED = ':lock: Only people on the domains team list can change or read the asset tracker. '
  + 'Ask an admin to add your Slack member id to DOMAINS_ALLOWED_USERS.';

/**
 * Register the tracker features on a Bolt app.
 *   opts.botToken  token for CSV downloads
 *   opts.command   slash command to answer ('/infra'), or null to skip
 *   opts.messages  true = handle CSV / renew / delete messages here
 *                  (Domain Suggester routes DMs itself and calls handleMessage)
 *   opts.allowed   (userId) => bool. Default: the domains team allow-list
 *                  (DOMAINS_ALLOWED_USERS + approvers). Until 2026-10-08 anyone
 *                  in the workspace could delete tracker rows with a message.
 */
function registerInfraHandlers(app, deps, opts = {}) {
  const infra = makeInfra(deps);
  const allowed = opts.allowed || (u => require('./domains_access').isAllowed(u));
  const deny = (client, user) => client.chat.postMessage({ channel: user, text: LOCKED }).catch(() => {});
  if (opts.command) {
    app.command(opts.command, async ({ ack, body, client, command, respond }) => {
      await ack();
      if (!allowed(body.user_id)) return respond({ response_type: 'ephemeral', text: LOCKED });
      return runInfraText(command.text, { client, trigger_id: body.trigger_id, user: body.user_id, respond }, infra);
    });
  }
  if (opts.messages) {
    app.message(async ({ message, client }) => {
      try {
        if (message.subtype && message.subtype !== 'file_share') return;
        if (message.bot_id || !isInfraMessage(message)) return;
        if (!allowed(message.user)) {
          return client.chat.postMessage({ channel: message.channel, text: LOCKED });
        }
        await infra.handleMessage(message, text => client.chat.postMessage({ channel: message.channel, text }), opts.botToken);
      } catch (err) {
        console.error('Message handler error:', err);
        try { await client.chat.postMessage({ channel: message.channel, text: `Error: ${err.message}` }); } catch (_) { /* ignore */ }
      }
    });
  }
  const guardView = handler => async args => {
    const user = args.body && args.body.user && args.body.user.id;
    if (!allowed(user)) { await args.ack(); return deny(args.client, user); }
    return handler(args);
  };
  const guardAction = handler => async args => {
    const user = args.body && args.body.user && args.body.user.id;
    if (!allowed(user)) {
      await args.ack();
      return args.respond ? args.respond({ response_type: 'ephemeral', replace_original: false, text: LOCKED }) : deny(args.client, user);
    }
    return handler(args);
  };
  app.view('ADD_ASSET_MODAL', guardView(infra.onAddSubmit));
  app.view('RENEW_ASSET_MODAL', guardView(infra.onRenewSubmit));
  app.action('infra_add_open', guardAction(async ({ ack, body, client }) => {
    await ack();
    await client.views.open({ trigger_id: body.trigger_id, view: addAssetModal() });
  }));
  app.action('infra_renew_open', guardAction(async ({ ack, body, client }) => {
    await ack();
    await client.views.open({ trigger_id: body.trigger_id, view: renewAssetModal() });
  }));
  app.action('infra_list', guardAction(async ({ ack, body, client }) => {
    await ack();
    await client.chat.postMessage({ channel: body.user.id, text: await infra.listText() });
  }));
  app.action(/^infra_expiring_\d+$/, guardAction(async ({ ack, action, respond }) => {
    await ack();
    const days = Math.min(60, parseInt(String(action.action_id).split('_').pop(), 10) || 7);
    await respond({ response_type: 'ephemeral', replace_original: false, text: 'Expiring assets',
      blocks: expiringBlocks(await infra.expiring(days), days) });
  }));
  app.action('infra_mark_renewed', guardAction(async ({ ack, body, action, respond }) => {
    await ack();
    let names = [];
    try { names = (JSON.parse(action.value).names || []).filter(n => typeof n === 'string' && /^[a-z0-9@._+-]{3,120}$/i.test(n)); } catch (e) { names = []; }
    if (!names.length) return;
    const { renewed, notFound } = await infra.markRenewed(names);
    await respond({ response_type: 'ephemeral', replace_original: false,
      text: '✅ <@' + body.user.id + '> marked ' + renewed + ' row(s) renewed in the tracker' + (notFound ? ', ' + notFound + ' not found' : '') + '. Google Sheets synced ✓' });
  }));
  return infra;
}

/** `/infra add | renew | list` (and the same words after `/domains infra`). */
async function runInfraText(text, { client, trigger_id, user, respond }, infra) {
  const t = String(text || '').trim().toLowerCase();
  if ((t === 'add' || t === 'renew') && !trigger_id && respond) {
    // Typed in a DM: Slack gives no trigger to open a form, so offer a button.
    return respond({ text: 'Open the form', blocks: [{ type: 'actions', elements: [{ type: 'button',
      action_id: t === 'add' ? 'infra_add_open' : 'infra_renew_open',
      text: { type: 'plain_text', text: t === 'add' ? 'Add an asset' : 'Renew an asset' }, value: t }] }] });
  }
  if (t === 'add') return client.views.open({ trigger_id, view: addAssetModal() });
  if (t === 'renew') return client.views.open({ trigger_id, view: renewAssetModal() });
  if (t === 'list') return client.chat.postMessage({ channel: user, text: await infra.listText() });
  const m = /^expiring(?:\s+(\d{1,2}))?$/.exec(t);
  if (m && respond) {
    const days = Math.min(60, parseInt(m[1] || '7', 10));
    return respond({ response_type: 'ephemeral', text: 'Expiring assets', blocks: expiringBlocks(await infra.expiring(days), days) });
  }
  if (respond) {
    return respond({ response_type: 'ephemeral', text: '*Tracker* — `infra add` · `infra renew` · `infra list` · `infra expiring 7`'
      + NL + 'Or send me a CSV (`domains.csv`, `renew_inboxes.csv`, `delete_domains.csv`), or a message starting with `renew` / `delete` and one name per line.' });
  }
  return null;
}

module.exports = { registerInfraHandlers, makeInfra, runInfraText, expiringBlocks, addAssetModal, renewAssetModal, isInfraMessage, LOCKED };
