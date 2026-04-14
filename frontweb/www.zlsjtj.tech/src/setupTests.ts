import '@testing-library/jest-dom/vitest';

// Ant Design uses matchMedia for responsive observers.
Object.defineProperty(window, 'matchMedia', {
  writable: true,
  value: (query: string) => ({
    matches: false,
    media: query,
    onchange: null,
    addListener: () => void 0,
    removeListener: () => void 0,
    addEventListener: () => void 0,
    removeEventListener: () => void 0,
    dispatchEvent: () => false,
  }),
});

// Some libs (e.g. Ant Design portal/scroll locker) call `getComputedStyle(el, pseudoElt)`,
// but jsdom doesn't implement the 2nd arg. Ignore the pseudo element for tests.
const _getComputedStyle = window.getComputedStyle;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
Object.defineProperty(window, 'getComputedStyle', {
  configurable: true,
  value: (elt: Element, _pseudoElt?: string) => _getComputedStyle(elt as any),
});
