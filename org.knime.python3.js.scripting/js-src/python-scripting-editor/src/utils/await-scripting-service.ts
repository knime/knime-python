import { ref } from 'vue';
import { createScriptingService } from '@/utils/python-scripting-service';
// import { createScriptingService } from '@/utils/python-mock-scripting-service';
import type { PythonScriptingService } from './python-scripting-service';
import type { Ref } from 'vue';

export const pythonScriptingService: Ref<PythonScriptingService | null > = ref(null);

export const waitscriptingService = async () => {
    pythonScriptingService.value = await createScriptingService();
};
