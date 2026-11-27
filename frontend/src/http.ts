/** Cookie-authenticated manager requests. Refresh CSRF before each mutation. */
export function createApi(fetcher: typeof fetch = fetch) {
  async function perform(path: string, method: string, body?: unknown, csrf?: string) {
    const headers: Record<string, string> = {
      'Content-Type': body instanceof Blob ? 'application/octet-stream' : 'application/json',
    };
    if (csrf) headers['X-Pero-CSRF'] = csrf;
    const response = await fetcher('/manage/v1' + path, {
      method,
      credentials: 'same-origin',
      headers,
      ...(body !== undefined ? {body: body instanceof Blob ? body : JSON.stringify(body)} : {}),
    });
    if (!response.ok) {
      const detail = await response.json().catch(() => ({}));
      throw new Error(detail?.detail?.code || `HTTP ${response.status}`);
    }
    return response.json();
  }
  return async function api(path: string, method = 'GET', body?: unknown) {
    method = method.toUpperCase();
    const mutation = !['GET', 'HEAD', 'OPTIONS'].includes(method);
    // Login establishes the session; all other mutations, including logout,
    // must use the token belonging to the cookie currently in this browser.
    let token: string | undefined;
    if (mutation && !(path === '/auth/session' && method === 'POST')) {
      const session = await perform('/auth/session', 'GET');
      if (!session.authenticated || typeof session.csrf_token !== 'string' || !session.csrf_token) {
        throw new Error('csrf_required');
      }
      token = session.csrf_token;
    }
    // No automatic mutation retry: credential creation must never be duplicated.
    return perform(path, method, body, token);
  };
}
