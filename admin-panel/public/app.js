const state = {
  selectedPlayerId: null,
  commandPermissions: {},
};

const els = {
  searchInput: document.getElementById('searchInput'),
  searchBtn: document.getElementById('searchBtn'),
  playersBody: document.querySelector('#playersTable tbody'),
  selectedHint: document.getElementById('selectedHint'),
  editor: document.getElementById('editor'),
  playerTitle: document.getElementById('playerTitle'),
  levelInput: document.getElementById('levelInput'),
  setLevelBtn: document.getElementById('setLevelBtn'),
  coinsInput: document.getElementById('coinsInput'),
  setCoinsBtn: document.getElementById('setCoinsBtn'),
  villageThemeInput: document.getElementById('villageThemeInput'),
  setVillageThemeBtn: document.getElementById('setVillageThemeBtn'),
  resetVillageBtn: document.getElementById('resetVillageBtn'),
  roleVip: document.getElementById('roleVip'),
  roleModerator: document.getElementById('roleModerator'),
  roleAdmin: document.getElementById('roleAdmin'),
  saveRolesBtn: document.getElementById('saveRolesBtn'),
  badgeCategory: document.getElementById('badgeCategory'),
  badgeType: document.getElementById('badgeType'),
  badgeValue: document.getElementById('badgeValue'),
  upsertBadgeBtn: document.getElementById('upsertBadgeBtn'),
  deleteBadgeBtn: document.getElementById('deleteBadgeBtn'),
  badgesList: document.getElementById('badgesList'),
  commandSetCoinsRole: document.getElementById('commandSetCoinsRole'),
  commandSetLevelRole: document.getElementById('commandSetLevelRole'),
  saveCommandPermissionsBtn: document.getElementById('saveCommandPermissionsBtn'),
  globalPopupText: document.getElementById('globalPopupText'),
  sendGlobalPopupBtn: document.getElementById('sendGlobalPopupBtn'),
  status: document.getElementById('status'),
};

function setStatus(message, isError = false) {
  els.status.textContent = message;
  els.status.style.color = isError ? '#cc2936' : '#5c6c85';
}

async function api(path, options = {}) {
  const res = await fetch(path, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || `Request failed: ${res.status}`);
  return data;
}

function renderPlayers(players) {
  els.playersBody.innerHTML = '';
  for (const p of players) {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${p.user_id}</td>
      <td>${p.username}</td>
      <td>${p.village_name} (#${p.village_id})</td>
      <td>${p.level}</td>
      <td>${p.slx_credits}</td>
      <td>${p.badge_count}</td>
    `;
    tr.addEventListener('click', () => loadPlayer(p.user_id));
    els.playersBody.appendChild(tr);
  }
}

function renderPlayer(player) {
  state.selectedPlayerId = player.user_id;
  els.selectedHint.textContent = `Selected userId ${player.user_id}`;
  els.editor.style.display = 'block';
  els.playerTitle.textContent = `${player.username} | Level: ${player.level} | Coins: ${player.slx_credits} | Theme: ${player.village_theme}`;
  els.levelInput.value = String(player.level);
  els.coinsInput.value = String(player.slx_credits);
  els.villageThemeInput.value = String(player.village_theme ?? 0);
  els.roleVip.checked = Boolean(player.roles && player.roles.vip);
  els.roleModerator.checked = Boolean(player.roles && player.roles.moderator);
  els.roleAdmin.checked = Boolean(player.roles && player.roles.admin);

  els.badgesList.innerHTML = '';
  if (!player.badges || player.badges.length === 0) {
    els.badgesList.textContent = 'No badges rows.';
    return;
  }

  for (const b of player.badges) {
    const div = document.createElement('div');
    div.className = 'pill';
    div.textContent = `cat:${b.category} type:${b.type} value:${b.value}`;
    div.addEventListener('click', () => {
      els.badgeCategory.value = b.category;
      els.badgeType.value = b.type;
      els.badgeValue.value = b.value;
    });
    els.badgesList.appendChild(div);
  }
}

async function searchPlayers() {
  try {
    const q = encodeURIComponent(els.searchInput.value.trim());
    const data = await api(`/api/players?query=${q}&limit=100`);
    renderPlayers(data.players || []);
    setStatus(`Loaded ${data.players.length} players.`);
  } catch (err) {
    setStatus(err.message, true);
  }
}

function renderCommandPermissions(commands) {
  const byName = new Map((commands || []).map((entry) => [String(entry.command || '').toLowerCase(), entry.requiredRole]));
  const setcoins = byName.get('setcoins') || 'mod';
  const setlevel = byName.get('setlevel') || 'mod';
  state.commandPermissions = { setcoins, setlevel };
  els.commandSetCoinsRole.value = setcoins;
  els.commandSetLevelRole.value = setlevel;
}

async function loadCommandPermissions() {
  try {
    const data = await api('/api/chat-commands');
    renderCommandPermissions(data.commands || []);
  } catch (err) {
    setStatus(err.message, true);
  }
}

async function loadPlayer(playerId) {
  try {
    const data = await api(`/api/players/${playerId}`);
    renderPlayer(data.player);
    setStatus(`Loaded ${data.player.username}`);
  } catch (err) {
    setStatus(err.message, true);
  }
}

async function setCoins() {
  if (!state.selectedPlayerId) return;
  try {
    const slxCredits = Number(els.coinsInput.value);
    const data = await api(`/api/players/${state.selectedPlayerId}/coins`, {
      method: 'POST',
      body: JSON.stringify({ slxCredits }),
    });
    renderPlayer(data.player);
    setStatus(`Updated coins for user ${state.selectedPlayerId}`);
    await searchPlayers();
  } catch (err) {
    setStatus(err.message, true);
  }
}

async function setLevel() {
  if (!state.selectedPlayerId) return;
  try {
    const level = Number(els.levelInput.value);
    const data = await api(`/api/players/${state.selectedPlayerId}/level`, {
      method: 'POST',
      body: JSON.stringify({ level }),
    });
    renderPlayer(data.player);
    setStatus(`Updated level for user ${state.selectedPlayerId}`);
    await searchPlayers();
  } catch (err) {
    setStatus(err.message, true);
  }
}

async function setVillageTheme() {
  if (!state.selectedPlayerId) return;
  try {
    const villageTheme = Number(els.villageThemeInput.value);
    const data = await api(`/api/players/${state.selectedPlayerId}/village-theme`, {
      method: 'POST',
      body: JSON.stringify({ villageTheme }),
    });
    renderPlayer(data.player);
    setStatus(`Updated village theme for user ${state.selectedPlayerId}`);
    await searchPlayers();
  } catch (err) {
    setStatus(err.message, true);
  }
}

async function resetVillage() {
  if (!state.selectedPlayerId) return;
  const confirmed = window.confirm(
    `Reset all village data for user ${state.selectedPlayerId}? This deletes buildings, trees, NPCs, and custom heightmap data.`
  );
  if (!confirmed) return;

  try {
    const data = await api(`/api/players/${state.selectedPlayerId}/village-reset`, {
      method: 'POST',
      body: JSON.stringify({}),
    });
    renderPlayer(data.player);
    setStatus(`Village reset for user ${state.selectedPlayerId}`);
    await searchPlayers();
  } catch (err) {
    setStatus(err.message, true);
  }
}

async function saveRoles() {
  if (!state.selectedPlayerId) return;
  try {
    const body = {
      vip: els.roleVip.checked,
      moderator: els.roleModerator.checked,
      admin: els.roleAdmin.checked,
    };
    const data = await api(`/api/players/${state.selectedPlayerId}/roles`, {
      method: 'POST',
      body: JSON.stringify(body),
    });
    renderPlayer(data.player);
    setStatus(`Roles updated for user ${state.selectedPlayerId}`);
    await searchPlayers();
  } catch (err) {
    setStatus(err.message, true);
  }
}

async function upsertBadge() {
  if (!state.selectedPlayerId) return;
  try {
    const body = {
      category: Number(els.badgeCategory.value),
      type: Number(els.badgeType.value),
      value: Number(els.badgeValue.value),
    };
    const data = await api(`/api/players/${state.selectedPlayerId}/badges`, {
      method: 'POST',
      body: JSON.stringify(body),
    });
    renderPlayer(data.player);
    setStatus(`Badge upserted for user ${state.selectedPlayerId}`);
    await searchPlayers();
  } catch (err) {
    setStatus(err.message, true);
  }
}

async function deleteBadge() {
  if (!state.selectedPlayerId) return;
  try {
    const body = {
      category: Number(els.badgeCategory.value),
      type: Number(els.badgeType.value),
    };
    const data = await api(`/api/players/${state.selectedPlayerId}/badges`, {
      method: 'DELETE',
      body: JSON.stringify(body),
    });
    renderPlayer(data.player);
    setStatus(`Badge deleted for user ${state.selectedPlayerId}`);
    await searchPlayers();
  } catch (err) {
    setStatus(err.message, true);
  }
}

async function saveCommandPermissions() {
  try {
    const setCoinsRequiredRole = String(els.commandSetCoinsRole.value || 'mod');
    const setLevelRequiredRole = String(els.commandSetLevelRole.value || 'mod');

    await api('/api/chat-commands/setcoins', {
      method: 'POST',
      body: JSON.stringify({ requiredRole: setCoinsRequiredRole }),
    });
    const data = await api('/api/chat-commands/setlevel', {
      method: 'POST',
      body: JSON.stringify({ requiredRole: setLevelRequiredRole }),
    });
    renderCommandPermissions(data.commands || []);
    setStatus('Chat command permissions updated.');
  } catch (err) {
    setStatus(err.message, true);
  }
}

async function sendGlobalPopup() {
  try {
    const messageText = String(els.globalPopupText.value || '').trim();
    if (messageText === '') {
      setStatus('Popup message cannot be empty.', true);
      return;
    }

    await api('/api/popups/global', {
      method: 'POST',
      body: JSON.stringify({ messageText }),
    });
    els.globalPopupText.value = '';
    setStatus('Global popup queued for online players.');
  } catch (err) {
    setStatus(err.message, true);
  }
}

els.searchBtn.addEventListener('click', searchPlayers);
els.searchInput.addEventListener('keydown', (e) => {
  if (e.key === 'Enter') searchPlayers();
});
els.setLevelBtn.addEventListener('click', setLevel);
els.setCoinsBtn.addEventListener('click', setCoins);
els.setVillageThemeBtn.addEventListener('click', setVillageTheme);
els.resetVillageBtn.addEventListener('click', resetVillage);
els.saveRolesBtn.addEventListener('click', saveRoles);
els.upsertBadgeBtn.addEventListener('click', upsertBadge);
els.deleteBadgeBtn.addEventListener('click', deleteBadge);
els.saveCommandPermissionsBtn.addEventListener('click', saveCommandPermissions);
els.sendGlobalPopupBtn.addEventListener('click', sendGlobalPopup);
els.globalPopupText.addEventListener('keydown', (e) => {
  if (e.key === 'Enter') sendGlobalPopup();
});

searchPlayers();
loadCommandPermissions();
